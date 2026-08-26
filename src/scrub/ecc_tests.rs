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

/// A SINGLE-data-block ECC SST, so the heal's positioned-read sequence is
/// predictable for fault-injection tests: the up-front correction-prediction
/// pass reads the block twice (scrub + re-read) and the write-back pass reads it
/// twice more, in that order.
fn write_single_block_ecc_sst(
    dir: &std::path::Path,
) -> (std::path::PathBuf, crate::table::BlockHandle) {
    let tree = open_ecc_tree(dir);
    for i in 0u64..4 {
        tree.insert(format!("key-{i:03}"), format!("v{i:03}"), i);
    }
    tree.flush_active_memtable(4).expect("flush");

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

/// As [`write_ecc_sst`], but with per-KV checksum footers
/// (`KvChecksumPolicy::AllLevels`). Footered tables keep the stale-digest
/// reconcile available on a later clean pass (their value bytes re-derive
/// authentication through the per-KV gate), so reconcile tests use this
/// fixture.
fn write_ecc_sst_footered(
    dir: &std::path::Path,
) -> (std::path::PathBuf, crate::table::BlockHandle) {
    let tree = open_ecc_tree(dir);
    tree.update_runtime_config(|c| {
        c.kv_checksums = crate::runtime_config::KvChecksumPolicy::AllLevels;
    })
    .expect("enable kv checksums");
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

/// Rebuilds the manifest by hand, recording the digest of the CURRENT
/// (possibly rotted) bytes of the single table under `tables/`. This seeds the
/// state the reconcile tests need — a manifest digest that matches damaged
/// bytes exactly (as when the damage lands before the digest is first
/// recorded) — which `Config::repair()` deliberately refuses to produce: its
/// block verification drops a table whose data blocks do not verify
/// rather than blessing a laundered digest.
fn rebuild_manifest_over_current_bytes(dir: &std::path::Path) -> crate::Result<()> {
    use crate::version::{Level, Run, Version};
    use std::sync::Arc;

    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let sst_path = dir.join("tables").join("0");
    let checksum =
        crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &sst_path)?);
    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::Metrics::default());
    let table = {
        #[cfg_attr(not(feature = "metrics"), expect(unused_mut))]
        let mut params = crate::table::RecoverParams::new(
            sst_path,
            checksum,
            0,
            Arc::clone(&fs),
            crate::comparator::default_comparator(),
            Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        );
        #[cfg(feature = "metrics")]
        {
            params.metrics = metrics;
        }
        crate::table::Table::recover(params)?
    };

    // Remove the prior snapshots so the hand-built one is the newest.
    let mut next_version_id = 0u64;
    for entry in fs.read_dir(dir)? {
        if let Some(rest) = entry.file_name.strip_prefix('v')
            && let Ok(n) = rest.parse::<u64>()
        {
            next_version_id = next_version_id.max(n + 1);
        }
    }

    let run = Run::new(alloc::vec![table]).expect("a non-empty run");
    let mut levels = alloc::vec![Level::from_runs(alloc::vec![Arc::new(run)])];
    for _ in 1..7 {
        levels.push(Level::empty());
    }
    let version = Version::from_levels(
        next_version_id,
        crate::config::TreeType::Standard,
        levels,
        crate::version::BlobFileList::new(crate::HashMap::default()),
        crate::blob_tree::FragmentationMap::default(),
    );
    crate::version::persist_version(
        dir,
        &version,
        crate::comparator::default_comparator().name(),
        &*fs,
        Arc::new(crate::runtime_config::types::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Full,
    )?;
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
    let (_sst_path, _block) = write_single_block_ecc_sst(dir.path());

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The single block is read twice in the up-front correction-prediction pass
    // (scrub, then the raw frame re-read for the parity-trailer comparison) and
    // twice again in the write-back pass. Skip the two prediction reads and the
    // write-back scrub, then fail exactly the write-back parity re-read.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(3)
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

    // The rot leaves the file differing from the (un-rebuilt) manifest, so this is
    // the restorative heal path: the FIRST write to `tables/` is the crash-recovery
    // marker sidecar, the SECOND is the trailer rebuild. Let the marker land, then
    // fail the trailer write-back.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
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

/// A write-back that FAILS must KEEP the in-progress marker, even though zero
/// blocks were counted as healed. `write_all` reports no byte count on error, so
/// a failure may have PARTIALLY written the block (or a full write's later sync
/// failed while the bytes still reach storage): the file may already differ from
/// the manifest digest, and dropping the marker would strand it with no
/// attribution for a later refresh. Removal is reserved for the no-mutation case
/// (a block that was never written — see the sibling uncorrectable test).
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_keeps_the_marker_when_a_write_back_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    // Rot a parity trailer (payload checksum still verifies, so the block scrubs
    // Clean and the trailer-rebuild heal fires) and rebuild the manifest over
    // the rotted bytes, so the pre-heal digest matches and the marker is written.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The FIRST write to `tables/` is the marker sidecar; the SECOND is the
    // trailer rebuild. Skip the marker write so the marker lands, then fail the
    // trailer write so zero blocks heal but a write WAS attempted.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "a failed write-back heals no block: {report:?}",
    );
    assert!(
        report.uncorrectable_blocks >= 1
            && format!("{report:?}").contains("in-place parity rebuild"),
        "the failed trailer write-back must be recorded, proving the write was \
         attempted: {report:?}",
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the marker must be KEPT after a failed write-back — the file may already be \
         partially modified, so a later patrol still needs the attribution",
    );
    Ok(())
}

/// The RESTORATIVE heal path (the current bytes already differ from the manifest
/// digest, but healing restores exactly what the manifest describes) must ALSO
/// persist its `.heal-attest` marker BEFORE the first write-back. Otherwise a
/// crash after syncing some of several corrections leaves the file matching
/// neither the manifest nor the healed digest, and with no marker a checkpoint
/// hard-links those intermediate bytes under the stale manifest digest, producing
/// a permanently inconsistent checkpoint. Rot a parity trailer but leave the
/// manifest holding the ORIGINAL digest (so the heal is restorative, not
/// attributable), fault the write-back after the marker lands, and assert the
/// marker persists (#78).
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_keeps_the_marker_when_a_restorative_write_back_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    // Rot a parity trailer but DO NOT rebuild the manifest: the current bytes now
    // differ from the manifest digest (pre-heal does NOT match), yet rebuilding the
    // trailer restores exactly the original bytes the manifest still describes
    // (predicted == manifest): the restorative path.
    corrupt_parity_trailer_byte(&sst_path, &block)?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The FIRST write to `tables/` is the marker sidecar; the SECOND is the trailer
    // rebuild. Let the marker land, then fail the write-back so the file may be
    // partially modified with the marker still present.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "a failed write-back heals no block: {report:?}",
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the restorative heal must persist its marker BEFORE the first write-back, so a \
         crash cannot expose unattested intermediate bytes to a checkpoint hard-link",
    );
    Ok(())
}

/// The marker IS removed when no write was ever attempted: every candidate block
/// is uncorrectable, so the file is untouched and the marker attests to a heal
/// that never happened. Removing it prevents its unexpiring `pre == manifest`
/// binding from later authorizing an unrelated digest mismatch.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_removes_the_marker_when_no_write_is_attempted() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    // Wreck the ENTIRE first data block (payload + parity trailer) BEYOND RS
    // recovery so it scrubs uncorrectable and the heal reaches no write-back at
    // all, then rebuild the manifest so the pre-heal digest matches and the
    // marker is written up front.
    let start = block.offset().0 as usize + Header::MIN_LEN;
    let end = block.offset().0 as usize + block.size() as usize;
    let mut bytes = std::fs::read(&sst_path)?;
    for off in start..end {
        if let Some(b) = bytes.get_mut(off) {
            *b ^= 0xFF;
        }
    }
    std::fs::write(&sst_path, &bytes)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "an uncorrectable block heals nothing: {report:?}",
    );
    assert!(
        report.uncorrectable_blocks >= 1,
        "the uncorrectable block is recorded: {report:?}",
    );
    assert!(
        !heal_attest_path(&sst_path).exists(),
        "the marker must be removed when no write was attempted, so it cannot authorize a \
         later unrelated mismatch",
    );
    Ok(())
}

/// A heal that lands its block but then hits a TRANSIENT read error during the
/// out-of-band reconcile walk must KEEP the heal attestation. The block was
/// genuinely healed and the marker is its only durable attribution; deleting it
/// on an inconclusive (retryable) failure would strand the healed SST under the
/// stale manifest digest, and every later clean patrol would then reject the
/// reconcile forever. The marker is dropped only on PROVEN corruption.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_keeps_the_marker_when_the_reconcile_walk_read_fails_transiently()
-> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;

    // A SINGLE-data-block ECC SST: the heal scan then does exactly two SST reads
    // (the block scrub, then the persist-side re-read), so the NEXT read is the
    // reconcile walk — the read this test faults.
    let (sst_path, block) = {
        let tree = open_ecc_tree(dir.path());
        for i in 0u64..4 {
            tree.insert(format!("key-{i:03}"), format!("v{i:03}"), i);
        }
        tree.flush_active_memtable(4).expect("flush");
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
        (
            (*table.path).clone(),
            crate::table::BlockHandle::new(keyed.offset(), keyed.size()),
        )
    };

    // Rot the parity trailer (the payload stays checksum-clean, so the block
    // scrubs Clean and the trailer-rebuild heal fires) and rebuild the manifest
    // over the rotted bytes so the pre-heal digest matches and the marker lands.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The heal reads the block twice in the up-front correction-prediction pass
    // (scrub + parity re-read) and twice more in the write-back pass, so the
    // block's trailer rebuild lands after 4 positioned reads; every read after
    // that belongs to the reconcile walk / semantic checks (the digest passes
    // stream sequentially, not via ReadAt). Fail them all so the reconcile hits
    // a transient read error, which must NOT delete the just-written
    // attestation.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(4),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 1,
        "the trailer rebuild must land before the walk fault: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "the transient walk read must refuse the digest refresh: {report:?}",
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "a transient walk failure must KEEP the marker for retry, not strand the \
         healed SST under the stale manifest digest",
    );
    Ok(())
}

/// A TRANSIENT read during the up-front correction-PREDICTION pass must
/// PROPAGATE, not be folded into "no correction". Swallowing it would drop the
/// block from the predicted offset set, so the write pass — gated on that set —
/// would skip a correction it re-discovers on the healable block and report a
/// clean pass with the fault still on disk. Fault ONLY the first positioned read
/// (the prediction pass's initial block scrub); the write-pass reads then
/// succeed, so the pre-fix `Err(_) => Ok(None)` silently skips the heal while the
/// fixed code surfaces the transient read.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_propagates_a_transient_read_during_correction_prediction() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;

    // A single-data-block ECC SST whose parity trailer is rotted (payload stays
    // checksum-clean, so the block would heal via a trailer rebuild).
    let (sst_path, block) = {
        let tree = open_ecc_tree(dir.path());
        for i in 0u64..4 {
            tree.insert(format!("key-{i:03}"), format!("v{i:03}"), i);
        }
        tree.flush_active_memtable(4).expect("flush");
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
        (
            (*table.path).clone(),
            crate::table::BlockHandle::new(keyed.offset(), keyed.size()),
        )
    };
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // Fault the FIRST positioned read — the prediction pass's initial block
    // scrub — and let every later read succeed, so the write pass would find the
    // block healable but never see its offset in the predicted set.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    // The transient prediction read must SURFACE (an error / inconclusive pass),
    // not vanish into a clean report that leaves the fault on disk.
    assert!(
        !report.is_ok(),
        "a transient prediction read must surface as an error, not report a clean pass \
         over the un-healed fault: {report:?}",
    );
    Ok(())
}

/// The reconcile must ABORT if it cannot persist the crash-recovery attestation:
/// installing the refreshed digest while that marker's write failed would leave
/// the healed bytes with no on-disk marker for a crash mid-install, and a later
/// patrol would then refuse to attribute the mismatch. Fault the reconcile's
/// attestation write (the SECOND sidecar write; the up-front one during the heal
/// succeeds) and assert the refresh is refused with the marker kept for retry.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_refuses_the_refresh_when_the_reconcile_attestation_write_fails()
-> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;

    let (sst_path, block) = {
        let tree = open_ecc_tree(dir.path());
        for i in 0u64..4 {
            tree.insert(format!("key-{i:03}"), format!("v{i:03}"), i);
        }
        tree.flush_active_memtable(4).expect("flush");
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
        (
            (*table.path).clone(),
            crate::table::BlockHandle::new(keyed.offset(), keyed.size()),
        )
    };

    // Attributable trailer-rebuild heal (payload stays checksum-clean).
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The heal writes the completed marker UP FRONT (first sidecar write); the
    // reconcile re-writes it before installing (second sidecar write). Skip the
    // first and fail the second so only the reconcile's attestation write fails.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path(".heal-attest")
            .skip(1)
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 1,
        "the trailer rebuild lands before the reconcile: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a failed reconcile attestation write must refuse the digest refresh, not \
         install it without a durable marker: {report:?}",
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the marker is kept for the next patrol to retry the reconcile",
    );
    Ok(())
}

/// A tight-space RESTRICTED table's heal walk must SKIP the punched-out prefix:
/// a block whose last key is below the restriction bound was reclaimed by a
/// superseding output table, so reading its frame reports a spurious
/// uncorrectable error that would suppress the digest refresh for a real
/// correction in the live suffix. The walk must start at the bound.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_skips_blocks_below_the_restriction_bound() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, first_block) = write_ecc_sst(dir.path());

    // Wreck the FIRST data block's payload beyond RS recovery (uncorrectable):
    // it holds the lowest keys, so a bound above them puts it in the prefix.
    let start = first_block.offset().0 as usize + Header::MIN_LEN;
    let mut bytes = std::fs::read(&sst_path)?;
    for off in start..start + 256 {
        if let Some(b) = bytes.get_mut(off) {
            *b ^= 0xFF;
        }
    }
    std::fs::write(&sst_path, &bytes)?;

    // Re-open the table restricted to a bound well above the first block's keys,
    // so the wrecked block sits in the (punched) prefix the walk must skip.
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));
    let binding = tree.version_history.read().latest_version();
    let table = binding
        .version
        .iter_tables()
        .next()
        .expect("flush produced one table");
    let restricted = table.reopen_restricted(crate::UserKey::from(b"key-001000".as_slice()))?;

    let (report, _healed) =
        restricted.heal_data_blocks_in_place(crate::fs::SyncMode::Full, restricted.checksum());
    assert!(
        report.errors.is_empty()
            && report.uncorrectable_blocks == 0
            && report.blocks_healed_in_place == 0,
        "the wrecked block below the restriction bound must be skipped entirely, \
         neither healed nor reported: {report:?}",
    );
    Ok(())
}

/// EVERY table the tree makes reachable must carry the heal-hint sink,
/// whichever path published it. A table that skips it looks perfectly healthy
/// and fails silently much later: a confirmed-persistent ECC correction can
/// never queue it for a healing rewrite, so the bitrot stays on disk and every
/// read pays the correction again.
///
/// Publication happens from several places — flush, compaction, and bulk
/// ingest, which builds and installs its tables itself — and each one binding
/// the sinks by hand is what let two of them drift. This walks the live
/// version after exercising all three, so a future path that forgets is
/// caught here rather than in a support ticket.
#[cfg(feature = "page_ecc")]
#[test]
fn every_published_table_carries_the_heal_hint_sink() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempfile::tempdir()?;
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));

    // Flush, then compaction over two flushed tables.
    for round in 0..2u64 {
        for i in 0..32u64 {
            tree.insert(
                format!("key-{i:06}").as_bytes(),
                format!("v{round}").as_bytes(),
                round * 100 + i,
            );
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 1_000)?;

    // Bulk ingest, which publishes its tables without going through the
    // flush path's registration.
    let mut ingestion = crate::tree::ingest::Ingestion::new(&tree)?;
    for i in 0..16u64 {
        ingestion.write(
            format!("ingested-{i:06}").as_bytes().into(),
            format!("i{i}").as_bytes().into(),
        )?;
    }
    ingestion.finish()?;

    let live: Vec<_> = {
        let binding = tree.version_history.read().latest_version();
        binding.version.iter_tables().cloned().collect()
    };
    assert!(
        live.len() >= 2,
        "flush, compaction and ingest all published"
    );
    for table in &live {
        assert!(
            table
                .heal_hints_for_test()
                .is_some_and(|sink| std::sync::Arc::ptr_eq(&sink, &tree.heal_hints)),
            "live table {} carries no heal-hint sink: a correctable fault in \
             it could never schedule a durable heal",
            table.id(),
        );
    }
    Ok(())
}

/// Compaction installs its outputs directly instead of going through
/// `register_tables`, so it must install the same tree-wide sinks a flush
/// gets — the heal-hint sink included. Without it a confirmed-persistent ECC
/// correction while reading a compaction output corrects the block in memory
/// but can never queue that SST for a healing rewrite, so the bitrot stays on
/// disk indefinitely (and every later read pays the correction again).
#[cfg(feature = "page_ecc")]
#[test]
fn a_compaction_output_carries_the_heal_hint_sink() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempfile::tempdir()?;
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));

    // Two flushes, then a compaction that merges them into a fresh output.
    for round in 0..2u64 {
        for i in 0..64u64 {
            tree.insert(
                format!("key-{i:06}").as_bytes(),
                format!("v{round}").as_bytes(),
                round * 100 + i,
            );
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 1_000)?;

    let outputs: Vec<_> = {
        let binding = tree.version_history.read().latest_version();
        binding.version.iter_tables().cloned().collect()
    };
    assert!(!outputs.is_empty(), "the compaction produced an output");
    for table in &outputs {
        assert!(
            table
                .heal_hints_for_test()
                .is_some_and(|sink| { std::sync::Arc::ptr_eq(&sink, &tree.heal_hints) }),
            "compaction output {} must carry the tree's heal-hint sink, or a \
             correctable read from it can never schedule a durable heal",
            table.id(),
        );
    }
    Ok(())
}

/// A tight-space restricted reopen produces a DISTINCT `Inner`, so every
/// tree-installed shared gate must be carried forward — including the ECC
/// heal-hint sink. Without it, a correctable read from the restricted view
/// can no longer queue the table for a healing recompaction: persistent
/// bitrot keeps being corrected in memory on every read but is never
/// scheduled for a durable rewrite.
#[cfg(feature = "page_ecc")]
#[test]
fn restricted_reopen_carries_the_heal_hint_sink_forward() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (_sst_path, _block) = write_ecc_sst(dir.path());

    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));
    let table = {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table")
            .clone()
    };
    // The sink the owning tree installed (install one if the config left the
    // slot empty — the transfer contract is the same either way).
    table.install_heal_hints(crate::heal_hints::HealHints::new_shared(true));
    let installed = table.heal_hints_for_test().expect("the sink is installed");

    let restricted = table.reopen_restricted(crate::UserKey::from(b"key-001000".as_slice()))?;
    let carried = restricted.heal_hints_for_test();
    assert!(
        carried.is_some_and(|c| std::sync::Arc::ptr_eq(&c, &installed)),
        "the restricted reopen must carry the SAME heal-hint sink forward, or \
         correctable reads from the restricted view stop queueing the table \
         for a durable healing recompaction",
    );
    Ok(())
}

/// A heal whose manifest-digest refresh loses the compaction-state `try_lock`
/// (a concurrent compaction is mid-install; blocking would invert the
/// heal-lock / compaction-state order and deadlock) must NOT report a clean
/// pass: the healed bytes are durable but the manifest digest stays stale and
/// the attestation stays pending, so a later integrity check flags the mismatch
/// and a checkpoint can abort despite the "clean" scrub. The contention must
/// surface as a `ChecksumRefreshFailed` finding; the marker is kept for the
/// next patrol to reconcile.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_reports_a_contended_checksum_refresh_as_a_finding() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_single_block_ecc_sst(dir.path());

    // Attributable trailer-rebuild heal (payload stays checksum-clean): the
    // manifest digest covers the CURRENT (rotted) bytes, so the heal changes
    // them and the reconcile must install a refreshed digest.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));
    // Hold the compaction state across the scrub, as a long-running concurrent
    // compaction would: the reconcile's `try_lock` then loses.
    let _compaction_guard = tree.compaction_state.lock();

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert_eq!(
        report.blocks_healed_in_place, 1,
        "the heal itself lands; only the digest install is contended: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "install-lock contention must surface as a finding, not a clean pass: {report:?}",
    );
    assert!(!report.is_ok(), "{report:?}");
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the marker is kept for the next patrol to reconcile",
    );
    Ok(())
}

/// A patrol whose CAPTURED table view went stale — tight-space compaction
/// installed a RESTRICTED same-id view (whose manifest digest covers only the
/// live suffix) after the capture — must scan the CURRENT view, not the captured
/// one. Scanning the captured whole-file view against the current suffix
/// checksum makes the pre-heal digest probe fail unconditionally, so the
/// divergent-heal guard returns a default CLEAN report before the block walk:
/// the patrol claims `is_ok()` with zero blocks healed while the known
/// correctable fault stays on disk.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_scans_the_current_view_when_the_captured_one_went_stale() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _first_block) = write_ecc_sst(dir.path());

    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(crate::fs::StdFs));
    // Capture the UNRESTRICTED view, as a patrol does before its scan.
    let captured = {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table")
            .clone()
    };

    // A tight-space slice installs a RESTRICTED same-id view after the capture;
    // `with_tight_slice` is the worker's install transform. The restricted
    // view's manifest digest covers only the live suffix.
    let restricted = captured.reopen_restricted(crate::UserKey::from(b"key-001000".as_slice()))?;
    tree.version_history.write().upgrade_version(
        &tree.config.path,
        |current| {
            let mut copy = current.clone();
            let ctx = crate::version::TransformContext::new(tree.config.comparator.as_ref());
            copy.version = copy.version.with_tight_slice(
                &[(captured.id(), restricted.clone())],
                &[],
                &[],
                vec![],
                None,
                0,
                &ctx,
            );
            Ok(copy)
        },
        &tree.config.seqno,
        &tree.config.visible_seqno,
        &*tree.config.fs,
        tree.runtime_config.load_full(),
        tree.config.encryption.clone(),
    )?;

    // Rot one payload byte (RS-correctable) in the LAST data block — well above
    // the restriction bound, squarely inside the live suffix the current view
    // serves. The rot lands AFTER the restricted digest was captured, so the
    // heal is plainly restorative for the current view.
    let last_block = {
        let mut last = None;
        for handle in restricted.block_index.iter() {
            let handle = handle?;
            last = Some(crate::table::BlockHandle::new(
                handle.offset(),
                handle.size(),
            ));
        }
        last.expect("table has data blocks")
    };
    let corrupt_pos = last_block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(slot) = bytes.get_mut(corrupt_pos) else {
        panic!("corrupt_pos in range for the SST bytes");
    };
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // Scan through the STALE captured view. The restriction mismatch must make
    // the scan target the CURRENT view; a scan of the captured one would trip
    // the divergent-heal guard and report clean with the fault untouched.
    let report = super::scan_and_reconcile(
        &tree,
        &captured,
        &PatrolScrubOptions::default().heal_in_place(true),
    );
    assert!(
        report.blocks_healed_in_place >= 1,
        "the known correctable fault must be healed through the current view, \
         not silently skipped by the divergent-heal guard: {report:?}",
    );
    assert!(report.is_ok(), "{report:?}");
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
    let (sst_path, block) = write_single_block_ecc_sst(dir.path());

    // Flip one payload byte of the block (RS-correctable).
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

    // The corrupted first block is read twice in the up-front correction-
    // prediction pass (scrub + `heal_frame` re-read) and twice in the write-back
    // pass. Skip the two prediction reads and the write-back scrub, then fail the
    // write-back `heal_frame` re-read.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(3)
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

/// The digest reconciliation must not restamp over a FORGED VALUE in a
/// footer-less (default) table: a value byte changed behind a re-stamped
/// block checksum + parity decodes cleanly with the same keys, seqnos, and
/// counts, so every derived-metadata cross-check passes — the manifest
/// digest is the ONLY record of the original value bytes, and refreshing it
/// without attribution to this pass's own heal writes would erase that
/// record permanently.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_footerless_value() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_value_byte_in_first_data_block(&sst_path, Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged footer-less value must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would erase the only record of the original value bytes",
    );
    Ok(())
}

/// As [`write_ecc_sst_footered`], but with 256 KiB zstd data blocks over
/// ~600 KiB of KV so at least one data block splits into >= 2 inner zstd
/// blocks and the SST carries a `block_layout` section.
fn write_ecc_zstd_multiblock_sst(dir: &std::path::Path) -> std::path::PathBuf {
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
    .data_block_size_policy(crate::config::BlockSizePolicy::all(256 * 1024))
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::Zstd(19),
    ))
    .open()
    .expect("open ecc zstd tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree.update_runtime_config(|c| {
        c.kv_checksums = crate::runtime_config::KvChecksumPolicy::AllLevels;
    })
    .expect("enable kv checksums");
    for i in 0u64..20_000 {
        tree.insert(format!("key-{i:012}"), format!("value-{i:08}-payload"), i);
    }
    tree.flush_active_memtable(20_000).expect("flush");

    let binding = tree.version_history.read().latest_version();
    let table = binding
        .version
        .iter_tables()
        .next()
        .expect("flush produced one table");
    assert!(
        table.regions.block_layout.is_some(),
        "the multi-inner-block fixture must carry a block_layout section",
    );
    (*table.path).clone()
}

/// Salvage must NOT byte-copy a MULTI-INNER block verbatim: its recorded
/// `block_layout` is the very (checksum-consistent, unauthenticated) section a
/// forge can corrupt to route an otherwise-readable zstd SST through salvage, so
/// copying the block verbatim would re-emit the same untrusted inner boundaries
/// and keep partial range reads omitting keys even though salvage reports
/// success. The block re-encodes from the verified payload instead
/// (`verbatim = None`); single-inner blocks (empty layout) still copy verbatim.
#[test]
fn salvage_load_block_re_encodes_a_multi_inner_block() -> crate::Result<()> {
    use crate::table::BlockHandle;
    use crate::table::block::BlockType;

    let dir = tempfile::tempdir()?;
    // Build a multi-inner-block zstd SST and keep the tree alive so its table
    // handle stays valid for the salvage-load probe below.
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
    .data_block_size_policy(crate::config::BlockSizePolicy::all(256 * 1024))
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::Zstd(19),
    ))
    .open()
    .expect("open ecc zstd tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree.update_runtime_config(|c| {
        c.kv_checksums = crate::runtime_config::KvChecksumPolicy::AllLevels;
    })
    .expect("enable kv checksums");
    for i in 0u64..20_000 {
        tree.insert(format!("key-{i:012}"), format!("value-{i:08}-payload"), i);
    }
    tree.flush_active_memtable(20_000).expect("flush");

    let binding = tree.version_history.read().latest_version();
    let table = binding
        .version
        .iter_tables()
        .next()
        .expect("flush produced one table");
    let multi_inner_offsets = table.block_layout.offsets();
    assert!(
        !multi_inner_offsets.is_empty(),
        "the fixture must carry at least one multi-inner-block frame",
    );

    // The data block recorded in the block_layout is multi-inner: salvage must
    // re-encode it, not verbatim-copy its untrusted boundaries.
    let multi = table
        .block_index
        .iter()
        .filter_map(Result::ok)
        .find(|kh| multi_inner_offsets.contains(&kh.offset().0))
        .map(|kh| BlockHandle::new(kh.offset(), kh.size()))
        .expect("a block index handle for the multi-inner offset");
    let sb = table.salvage_load_block(&multi, BlockType::Data)?;
    assert!(
        sb.verbatim.is_none(),
        "a multi-inner block must re-encode from the verified payload, not byte-copy its \
         unauthenticated recorded layout",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `block_layout`:
/// a middle cumulative end shifted to another structurally valid value
/// (fresh block checksum + parity) passes every byte-level and framing
/// check — no gate compares the recorded boundaries with the zstd frames'
/// real inner-block layout — yet the partial range-read path trusts it to
/// bound decompression, silently omitting keys from the mis-mapped span.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_block_layout() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = write_ecc_zstd_multiblock_sst(dir.path());

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
    .data_block_size_policy(crate::config::BlockSizePolicy::all(256 * 1024))
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::Zstd(19),
    ))
    .open()
    .expect("reopen ecc zstd tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    crate::test_forge::forge_block_layout_shift_middle_end(&sst_path, Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged block_layout must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let partial range reads silently omit keys",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over TLI mirrors forged
/// CONSISTENTLY: both copies re-encoded to the same truncated handle list
/// (fresh checksums, parity, Index role) pass every byte-level check AND
/// the decoded mirror comparison — the equality of two forged copies proves
/// nothing. Only a structural check of the decoded handles against the
/// physical data section (the writer emits data blocks back-to-back, so the
/// handles must exactly TILE it) can catch the dropped handle before the
/// next recovery loads the forged list and range scans silently lose the
/// unreachable block. Footered fixture, so the forge is not pre-empted by
/// the footer-less attribution rule.
#[test]
fn heal_in_place_does_not_restamp_over_consistently_forged_tli_mirrors() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Open FIRST (the live table already loaded its index), then forge.
    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_tli_mirrors_truncated(
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
        "consistently forged TLI mirrors must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let the next recovery hide the dropped block",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over FORGED metadata KEY
/// BOUNDS: both meta mirrors re-stamped CONSISTENTLY to a narrower
/// `key#max` (fresh checksums and parity) pass every byte-level check and
/// the full mirror comparison — only a cross-check of the recorded range
/// against the decoded data keys can catch it. Restamping would make run
/// selection trust the forged range and silently skip this table for real
/// keys outside it. The fixture is footered, so the forge is not
/// pre-empted by the footer-less attribution rule.
#[test]
fn heal_in_place_does_not_restamp_over_forged_meta_key_bounds() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    let tree = open_ecc_tree(dir.path());
    // Real keys run up to "key-001999"; narrow the recorded max below half
    // the key space (same value length keeps the frame geometry).
    crate::test_forge::forge_meta_value_both_mirrors(&sst_path, b"key#max", b"key-000999")?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "forged meta key bounds must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let run selection silently skip real keys",
    );
    Ok(())
}

/// Opens an RS(8,2) Page-ECC tree at `dir` whose data blocks are
/// uncompressed and carry an embedded hash index — the layout
/// `forge_hash_index_all_free` requires.
fn open_ecc_hashed_tree(dir: &std::path::Path) -> crate::Tree {
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
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::None,
    ))
    .data_block_hash_ratio_policy(crate::config::HashRatioPolicy::all(2.0))
    .open()
    .expect("open ecc hashed tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree
}

/// A footered, uncompressed ECC SST whose data blocks carry an embedded
/// HASH INDEX (non-zero `data_block_hash_ratio`), for the hash-index forge.
fn write_ecc_sst_footered_hashed(dir: &std::path::Path) -> std::path::PathBuf {
    let tree = open_ecc_hashed_tree(dir);
    tree.update_runtime_config(|c| {
        c.kv_checksums = crate::runtime_config::KvChecksumPolicy::AllLevels;
    })
    .expect("enable kv checksums");
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
}

/// The digest reconciliation must not restamp over a FORGED embedded HASH
/// INDEX: filling the first block's hash index with `MARKER_FREE` leaves
/// every logical entry, per-KV footer, and the outer block checksum intact,
/// so a sequential decode and the count / key / seqno gates all pass — yet
/// after reopen `point_read` trusts the hash index and returns `None` for
/// every existing key in that block. The indexes must be probed against the
/// decoded keys before the digest is trusted.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_hash_index() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = write_ecc_sst_footered_hashed(dir.path());

    let tree = open_ecc_hashed_tree(dir.path());
    crate::test_forge::forge_hash_index_all_free(&sst_path, Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged hash index must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let point reads miss existing keys",
    );
    Ok(())
}

/// The reconcile gates must judge the bytes ON DISK, not the block cache:
/// a block read before the forge leaves its pristine copy cached, and a
/// gate that loads through the cache validates that stale original instead
/// of the file being reconciled. Same forge as
/// [`heal_in_place_does_not_restamp_over_a_forged_hash_index`], but with a
/// point read FIRST so the pristine first data block is cached when the
/// heal scan runs — the refresh must still be refused.
#[test]
fn heal_in_place_does_not_trust_cached_blocks_over_the_disk_bytes() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = write_ecc_sst_footered_hashed(dir.path());

    let tree = open_ecc_hashed_tree(dir.path());
    // Warm the block cache with the PRISTINE first data block (the key lives
    // in it), then forge the on-disk hash index; the cached copy stays good.
    assert!(
        tree.get("key-000000", MAX_SEQNO)?.is_some(),
        "pre-forge read warms the cache",
    );
    crate::test_forge::forge_hash_index_all_free(&sst_path, Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged hash index must refuse the digest refresh even when the \
         pristine block is cached: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let point reads miss existing keys after the cache cools",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over FORGED metadata SEQUENCE
/// bounds: on a footer-bearing table WITHOUT deletion metadata (no sentinel
/// to complicate the bounds), both meta mirrors re-stamped with `seqno#min`
/// raised while every data entry stays intact pass the mirror walk and all
/// per-KV / key / count gates — yet after reopen a snapshot read whose
/// threshold is at or below the forged minimum returns early at
/// `Table::get`, silently missing older visible versions. The recorded
/// bounds must be cross-checked against the decoded entries' real seqnos.
#[test]
fn heal_in_place_does_not_restamp_over_forged_meta_seqno_bounds() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Read the SST's real recorded max so the forged min stays <= max (a
    // min > max would fail the meta load for a different reason).
    let tree = open_ecc_tree(dir.path());
    let recorded_max = {
        let binding = tree.version_history.read().latest_version();
        let table = binding.version.iter_tables().next().expect("one table");
        table.get_highest_seqno()
    };
    // Raise seqno#min to the recorded max — every entry below it is now
    // hidden from snapshots at or under the forged minimum.
    crate::test_forge::forge_meta_value_both_mirrors(
        &sst_path,
        b"seqno#min",
        &recorded_max.to_le_bytes(),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "forged metadata seqno bounds must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let snapshot reads silently skip older visible versions",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `created_at`:
/// both meta mirrors re-stamped with an OLDER timestamp (same 16-byte
/// length, fresh checksums and parity) pass every byte-level check, the
/// mirror comparison, and every content-derived gate — no cross-check can
/// re-derive a wall-clock timestamp from the entries. Yet after reopen
/// FIFO compaction trusts the recorded `created_at` for its TTL decision
/// and can classify the live SST as expired, permanently dropping it. The
/// disk-fresh meta must equal the recovery-time copy field for field
/// before the digest is trusted.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_created_at() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Open FIRST so the live table keeps its recovery-time timestamp, then
    // back-date the on-disk copy in both mirrors (u128 LE nanoseconds; the
    // equal value length keeps the frame geometry).
    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_meta_value_both_mirrors(
        &sst_path,
        b"created_at",
        &1u128.to_le_bytes(),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged created_at must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let FIFO compaction drop the live SST as TTL-expired",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a `created_at` back-dated
/// while the tree was CLOSED. The post-open forge above is caught by the
/// field-for-field bounds check because the live table keeps the honest
/// recovery-time copy; an OFFLINE restamp defeats that check by poisoning the
/// recovery-time copy itself — recovery loads the forged `created_at`, so the
/// disk-fresh copy equals it. The manifest's whole-file digest is then the
/// only surviving record of the honest bytes, and the mismatch it produces is
/// UNATTRIBUTABLE to any heal. Because no cross-check can re-derive a
/// wall-clock timestamp, an unattributed mismatch must fail closed even on a
/// footer-bearing table; otherwise the patrol would persist the forged digest
/// and FIFO compaction would drop the live SST as TTL-expired.
#[test]
fn heal_in_place_rejects_a_created_at_restamped_before_open() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Back-date `created_at` in BOTH mirrors BEFORE the tree opens: recovery
    // then loads the forgery into the live table's metadata.
    crate::test_forge::forge_meta_value_both_mirrors(
        &sst_path,
        b"created_at",
        &1u128.to_le_bytes(),
    )?;

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    // Pin the fail-closed reason, not just the error variant: another gate
    // could raise ChecksumRefreshFailed for a different cause and keep this
    // test green while the attribution gate goes uncovered.
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ScrubError::ChecksumRefreshFailed { reason, .. }
                if reason.contains("not attributable to this pass's heal")
        )),
        "an unattributed mismatch on a footer-bearing table must not reconcile a \
         pre-open created_at restamp: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its digest \
         would let FIFO compaction drop the live SST as TTL-expired",
    );
    Ok(())
}

/// A stale IN-PROGRESS heal marker must NOT authorize a reconcile. The
/// in-progress marker binds only `pre == manifest`, not the healed bytes, so a
/// crash after the marker was written but before any block was healed leaves it
/// attesting a heal that never happened. If a checksum-restamped alteration to a
/// non-authenticatable surface (here a pre-open `created_at` back-date, which
/// poisons the recovery-time copy so no gate can catch it) then lands, the
/// marker would legitimize it: the patrol refreshes the manifest over the forged
/// bytes. Attribution must come only from this pass's heal or a COMPLETED marker
/// (which binds `post == current`); a bare pre-only marker is ignored, so the
/// mismatch stays unattributable and fails closed.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_in_place_ignores_a_stale_in_progress_marker() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Back-date `created_at` in BOTH mirrors before open, poisoning the
    // recovery-time copy so the field-for-field gate cannot catch it — the
    // mismatch is unattributable unless a marker authorizes it.
    crate::test_forge::forge_meta_value_both_mirrors(
        &sst_path,
        b"created_at",
        &1u128.to_le_bytes(),
    )?;

    let tree = open_ecc_tree(dir.path());
    // Manufacture a stale in-progress marker whose `pre` equals the manifest
    // digest (the clean, pre-forge checksum the manifest still records), as a
    // crash between the marker write and the first heal would leave.
    let manifest_checksum = {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table")
            .checksum()
    };
    crate::scrub::heal_attest::write_in_progress(
        &crate::fs::StdFs,
        &sst_path,
        None,
        0,
        manifest_checksum,
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ScrubError::ChecksumRefreshFailed { reason, .. }
                if reason.contains("not attributable to this pass's heal")
        )),
        "a stale in-progress marker must not make an unattributed mismatch \
         attributable: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: a stale in-progress \
         marker must not authorize restamping its digest",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED KV-footer
/// DESCRIPTOR: both meta mirrors re-stamped with `descriptor#kv_checksum`
/// set to off while the footer-bearing data blocks are left intact. The
/// mirror walk accepts the matching copies, and the in-memory descriptor is
/// still the recovery-time `Some(algo)` so `verify_kv_checksums` passes —
/// yet after reopen the on-disk `None` descriptor stops footer stripping,
/// so point reads misread footer bytes as the data-block trailer. The
/// disk-fresh descriptor must be cross-checked against the recovery-time
/// one before trusting the metadata.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_kv_checksum_descriptor() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    // Open FIRST so the live table keeps its recovery-time Some(algo), then
    // forge the on-disk descriptor to off (byte 0) in both mirrors.
    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_meta_value_both_mirrors(&sst_path, b"descriptor#kv_checksum", &[0])?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged kv-checksum descriptor must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let point reads misread footer bytes as the trailer",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED metadata BLOCK
/// COUNT: both mirrors re-stamped consistently to a smaller
/// `block_count#data` (fresh checksums and parity) pass every byte-level
/// check and the mirror comparison, and the bounds gate's key/item checks
/// stay clean (the blocks themselves are untouched) — yet `Table::scan`
/// hands the recorded count to the compaction scanner, which stops after
/// that many blocks: a rewrite silently drops every key in the omitted
/// tail. Footered fixture, so the forge is not pre-empted by the
/// footer-less attribution rule.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_data_block_count() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    let tree = open_ecc_tree(dir.path());
    // The fixture writes 9 data blocks; record 1 (little-endian u64, same
    // value length keeps the frame geometry).
    crate::test_forge::forge_meta_value_both_mirrors(
        &sst_path,
        b"block_count#data",
        &1u64.to_le_bytes(),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged data-block count must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let compaction scans silently drop the omitted blocks",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED TLI SEPARATOR
/// key: both mirrors re-encoded with the first block's separator lowered to
/// a truncated prefix stay equal, sorted, and section-tiling — yet after
/// reopen the index binary search routes keys in `(forged_separator,
/// real_last_key]` to the wrong block, so `point_read` misses existing
/// keys. The in-memory point-read reachability gate does not catch this on
/// the heal path (the live table keeps its correct recovery-time index), so
/// the disk-fresh separator must be cross-checked against the addressed
/// block's decoded final key.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_tli_separator() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_tli_mirrors_lower_first_separator(
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
        "a forged TLI separator must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let point reads miss keys routed to the wrong block",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED TLI BINARY
/// INDEX: both mirrors re-stamped with the last binary-index pointer
/// redirected to the first restart head leave the sequential entry stream
/// untouched, so mirror equality, section tiling, and every separator
/// cross-check pass — yet after reopen the index binary search trusts the
/// forged pointer and can start at the wrong restart head, silently
/// missing keys on seeks. Each disk-fresh pointer must be validated
/// against the sequentially derived restart heads.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_tli_binary_index() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_footered(dir.path());

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_tli_binary_index_pointer(
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
        "a forged TLI binary index must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let index seeks start at the wrong restart head",
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
/// payload altered to another parseable `BuRR` filter (fresh block checksum +
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

/// Byte path of the heal attestation sidecar next to an SST.
fn heal_attest_path(sst_path: &std::path::Path) -> std::path::PathBuf {
    let mut name = sst_path.as_os_str().to_os_string();
    name.push(".heal-attest");
    std::path::PathBuf::from(name)
}

/// A manifest-digest refresh that FAILED (or a crash after the heal's
/// `sync_data` but before the manifest update) leaves a stale digest. Because
/// the heal writes a sidecar ATTESTATION before the reconciliation, a later
/// clean heal-in-place scrub — which sees only clean blocks and cannot
/// attribute the mismatch to any write of its own — reconciles the digest via
/// that attestation instead of flagging the healed table as corrupt forever.
/// (For an unencrypted table the attestation is plaintext; forging it needs the
/// same directory write access that could re-stamp the SST directly, which is
/// outside the on-disk-tamper model the digest gate defends.)
#[test]
fn heal_in_place_reconciles_a_crashed_refresh_via_the_attestation() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // FIRST heal pass: the trailer is rebuilt in place and an attestation is
    // written, but the manifest refresh fails (injected fault on the edit-log
    // open), leaving the stale digest AND the attestation on disk.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");
    assert!(
        !report.is_ok(),
        "the failed refresh is a finding: {report:?}"
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the heal must leave an attestation for the crashed refresh",
    );

    // SECOND heal pass, fault gone: every block reads clean (nothing to heal),
    // and the manifest still carries the rotted digest. The attestation proves
    // the file is the healed version, so the mismatch is reconciled.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.is_ok(),
        "a crashed refresh must reconcile via the attestation on the next scrub: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "the reconciled digest matches the healed file, got {:?}",
        integrity.errors,
    );
    assert!(
        !heal_attest_path(&sst_path).exists(),
        "the attestation is consumed once its reconciliation lands",
    );
    Ok(())
}

/// A heal writes the completed attestation UP FRONT — before any block is
/// healed — binding the deterministic post-heal digest, so a crash anywhere in
/// the heal leaves that marker on disk. The next clean scrub reconciles via it:
/// the marker binds `post == current`, so once the file reaches the healed
/// state the mismatch is attributable and the structural gates re-verify before
/// the digest is trusted. Without it the clean re-scan cannot attribute the
/// mismatch and the stale digest is rejected forever.
#[test]
fn heal_in_place_reconciles_via_a_completed_marker() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // First heal pass with a failing edit-log: the blocks are healed and the
    // completed attestation is written, but the manifest refresh fails.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");

    // Model the crash window explicitly: the file is healed, and the completed
    // marker binds the manifest's (stale) `pre` to the healed file's `post`.
    let (table_id, manifest_digest) = {
        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("the healed table is still in the manifest");
        (table.id(), table.checksum())
    };
    let healed_digest = crate::Checksum::from_raw(crate::repair::compute_table_checksum(
        &crate::fs::StdFs,
        &sst_path,
    )?);
    std::fs::remove_file(heal_attest_path(&sst_path))?;
    crate::scrub::heal_attest::write(
        &crate::fs::StdFs,
        &sst_path,
        None,
        table_id,
        manifest_digest,
        healed_digest,
    )?;

    // Second pass, fault gone: every block reads clean (nothing to heal), so the
    // mismatch is attributable only via the completed marker.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert_eq!(
        report.blocks_healed_in_place, 0,
        "the second pass must find every block clean, so ONLY the marker attributes the \
         mismatch (a re-heal would make attribution direct and skip the marker path): {report:?}",
    );
    assert!(
        report.is_ok(),
        "a crash before the manifest refresh must reconcile via the completed \
         marker on the next scrub: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "the reconciled digest matches the healed file, got {:?}",
        integrity.errors,
    );
    assert!(
        !heal_attest_path(&sst_path).exists(),
        "the marker is consumed once its reconciliation lands",
    );
    Ok(())
}

/// `attests_post` is the discriminator the heal path uses to decide a not-matched
/// heal is NOT diverging: it matches only a COMPLETED marker recording exactly
/// `post` for this table, regardless of the marker's `pre`. A wrong post or a
/// wrong table id does not match, so a genuinely diverging heal is not mistaken
/// for a safe restore-to-an-attested-digest.
#[test]
fn attests_post_matches_only_the_recorded_completed_post() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.sst");
    std::fs::write(&path, b"x")?;

    let pre = crate::Checksum::from_raw(0x1111);
    let post = crate::Checksum::from_raw(0x2222);
    crate::scrub::heal_attest::write(&crate::fs::StdFs, &path, None, 7, pre, post)?;

    use crate::scrub::heal_attest::AttestResult;
    // The recorded post matches for the right table, whatever the `pre` was.
    assert!(matches!(
        crate::scrub::heal_attest::attests_post(&crate::fs::StdFs, &path, None, 7, post),
        AttestResult::Attests,
    ));
    // A different post does not match.
    assert!(matches!(
        crate::scrub::heal_attest::attests_post(
            &crate::fs::StdFs,
            &path,
            None,
            7,
            crate::Checksum::from_raw(0x3333),
        ),
        AttestResult::Absent,
    ));
    // A different table id does not match.
    assert!(matches!(
        crate::scrub::heal_attest::attests_post(&crate::fs::StdFs, &path, None, 8, post),
        AttestResult::Absent,
    ));
    Ok(())
}

/// A TRANSIENT read of the attestation sidecar must resolve to `Inconclusive`,
/// never `Absent`. Collapsing it to "does not attest" (the old `bool` return)
/// would make the diverging-heal check skip the heal, then let the reconcile
/// reread the now-readable marker, find it no longer matches the current bytes,
/// and delete a VALID marker — permanently stranding the healed table.
#[test]
fn attests_post_is_inconclusive_on_a_transient_sidecar_read() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::scrub::heal_attest::AttestResult;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.sst");
    std::fs::write(&path, b"x")?;
    let post = crate::Checksum::from_raw(0x2222);
    crate::scrub::heal_attest::write(
        &crate::fs::StdFs,
        &path,
        None,
        7,
        crate::Checksum::from_raw(0x1111),
        post,
    )?;

    // Fault the sidecar OPEN with a transient (non-NotFound) error: `read_sidecar`
    // maps that to `Inconclusive`, and `attests_post` must propagate it.
    let fault = FaultFs::new(crate::fs::StdFs);
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted)).on_path("heal-attest"),
    );

    assert!(
        matches!(
            crate::scrub::heal_attest::attests_post(&fault, &path, None, 7, post),
            AttestResult::Inconclusive,
        ),
        "a transient sidecar read must be Inconclusive, not Absent",
    );
    Ok(())
}

/// Without a valid attestation, an unattributable stale digest STILL fails
/// closed: the attestation is the only thing that lets a clean re-scan
/// reconcile, so deleting it (modelling a crash before the attestation was
/// written, or an offline restamp with no attestation at all) must restore the
/// fail-closed behavior — nothing else distinguishes the mismatch from an
/// offline restamp of the non-derivable meta scalars.
#[test]
fn heal_in_place_fails_closed_without_a_heal_attestation() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // First pass heals + attests, but the refresh fails.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");

    // Remove the attestation before the retry: the mismatch is now
    // unattributable with no evidence of a legitimate heal.
    std::fs::remove_file(heal_attest_path(&sst_path))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ScrubError::ChecksumRefreshFailed { reason, .. }
                if reason.contains("not attributable to this pass's heal")
        )),
        "without an attestation the stale digest must fail closed: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the unattested stale digest keeps flagging, got {:?}",
        integrity.errors,
    );
    Ok(())
}

/// A transiently-unreadable sidecar must NOT be treated as an absent marker on
/// the unattributable re-scan path: deleting it on a retryable read error would
/// strand the healed table under the stale digest forever. The read is
/// INCONCLUSIVE, so the reconcile fails closed (no digest refresh) but KEEPS the
/// marker for the next pass to retry.
#[test]
fn reconcile_keeps_the_marker_when_the_sidecar_read_is_inconclusive() -> crate::Result<()> {
    use crate::fs::{Fault, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // First pass heals + writes the attestation, but the manifest refresh fails,
    // leaving the marker on disk for a later pass to reconcile.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the heal left a marker"
    );

    // Second pass: the block is clean, so the mismatch is attributable only via
    // the marker, but its read fails transiently (an open error, not not-found).
    // That is inconclusive, not absent, so the marker must survive.
    injector
        .arm(FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).on_path(".heal-attest"));
    let _ = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert!(
        heal_attest_path(&sst_path).exists(),
        "an inconclusive sidecar read must keep the marker, not delete it",
    );
    Ok(())
}

/// A `.heal-attest` sidecar whose SST is absent from the recovered manifest is
/// an orphan: its table was retired (compacted away, its numeric file unlinked)
/// while the attestation lingered. Recovery must SWEEP the orphan rather than
/// skip it forever — an attestation can only ever reconcile a table that still
/// exists, so a leaked sidecar is dead weight that every future recovery scan
/// re-processes. A LIVE table's pending attestation must still be preserved.
#[test]
fn recovery_sweeps_an_orphaned_heal_attestation_but_keeps_a_live_one() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _block) = write_ecc_sst(dir.path());
    let tables_dir = sst_path.parent().expect("the SST lives in a tables folder");

    // An orphan sidecar for a table id that is NOT in the manifest.
    let orphan = tables_dir.join("99999.heal-attest");
    std::fs::write(&orphan, b"orphaned attestation")?;
    // A pending attestation for the LIVE SST (its id IS in the manifest).
    let live = heal_attest_path(&sst_path);
    std::fs::write(&live, b"live attestation")?;

    // Reopen: recovery scans the tables folder and reconciles sidecars.
    let _tree = open_ecc_tree(dir.path());

    assert!(
        !orphan.exists(),
        "recovery must sweep a sidecar whose table id is absent from the manifest",
    );
    assert!(
        live.exists(),
        "recovery must preserve a live table's pending attestation",
    );
    Ok(())
}

/// A prior reconciliation may have installed the refreshed checksum but crashed
/// (or its best-effort sidecar unlink transiently failed) before removing the
/// `.heal-attest` marker. The next reconciliation then finds the on-disk digest
/// already matches the manifest and must RECLAIM the now-obsolete marker:
/// leaving it makes every future checkpoint classify the table as pending and
/// run a full heal scan before snapshotting.
#[test]
fn reconcile_reclaims_an_obsolete_marker_when_the_digest_already_matches() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // A clean ECC table: its on-disk digest already matches the manifest.
    let (sst_path, _block) = write_ecc_sst(dir.path());

    // Plant a leftover sidecar, modelling a prior reconcile that installed the
    // digest but did not manage to remove its marker.
    let marker = heal_attest_path(&sst_path);
    std::fs::write(&marker, b"obsolete marker")?;
    assert!(marker.exists());

    // A heal-in-place patrol scrub: the table is clean, so the reconcile takes
    // the `fresh == current` branch, which must reclaim the obsolete marker.
    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.is_ok(),
        "a clean ECC table scrubs cleanly: {report:?}"
    );

    assert!(
        !marker.exists(),
        "an obsolete marker must be reclaimed once the digest already matches the manifest",
    );
    Ok(())
}

/// The checkpoint-time guard `abort_checkpoint_if_pending_heals` must not wedge
/// on an OBSOLETE marker: a prior reconcile installed the refreshed digest but
/// crashed before removing the sidecar, so the file ALREADY matches the
/// manifest. A build WITHOUT `page_ecc` never runs reconciliation to clear it,
/// so an unconditional abort would fail EVERY checkpoint forever. When the
/// live-region digest already agrees with the manifest the guard reclaims the
/// stale marker and proceeds; a genuine pending heal (a digest that does NOT
/// match) still aborts.
#[test]
fn abort_checkpoint_ignores_an_obsolete_marker_but_aborts_on_a_stale_digest() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // A clean ECC table: its on-disk digest already matches the manifest.
    let (sst_path, _block) = write_ecc_sst(dir.path());
    let marker = heal_attest_path(&sst_path);

    // Case 1: an obsolete leftover marker (a crash between the digest refresh
    // and the marker unlink). The guard must proceed and reclaim it.
    std::fs::write(&marker, b"obsolete marker")?;
    let tree = open_ecc_tree(dir.path());
    crate::scrub::abort_checkpoint_if_pending_heals(&tree, "obsolete-marker case")?;
    assert!(
        !marker.exists(),
        "an obsolete marker matching the manifest must be reclaimed, not wedge the checkpoint",
    );

    // Case 2: a genuine pending heal — the on-disk digest no longer matches the
    // manifest. Flip an interior data byte (length and trailer intact) so the
    // streamed digest diverges while the in-memory manifest entry is unchanged.
    std::fs::write(&marker, b"pending marker")?;
    let mut bytes = std::fs::read(&sst_path)?;
    if let Some(b) = bytes.get_mut(64) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;
    let err = crate::scrub::abort_checkpoint_if_pending_heals(&tree, "stale-digest case")
        .expect_err("a pending heal whose digest does not match the manifest must abort");
    assert!(
        matches!(err, crate::Error::Io(_)),
        "the abort is surfaced as an Io error, got {err:?}",
    );
    assert!(
        marker.exists(),
        "a genuine pending marker is kept for the next reconciliation",
    );
    Ok(())
}

/// A checkpoint must not hold the link window's write half while its flush
/// blocks on `compaction_state`: with Page ECC that closes a three-way cycle —
/// a tight-space compaction holds `compaction_state` while waiting for a
/// table's heal lock, and a heal patrol holds that heal lock while waiting for
/// the link window's read half. Orchestrates all three parties (the test
/// thread stands in for the compaction, holding `compaction_state` and then
/// wanting the heal lock) and asserts every one completes; pre-fix the trio
/// deadlocks and the test hangs into the harness timeout.
#[cfg(feature = "page_ecc")]
#[test]
fn checkpoint_flush_does_not_deadlock_with_patrol_and_compaction() -> crate::Result<()> {
    use crate::AbstractTree;
    use std::sync::Arc;
    use std::time::Duration;

    let dir = tempfile::tempdir()?;
    write_ecc_sst(dir.path());
    let tree = Arc::new(open_ecc_tree_on(dir.path(), Arc::new(crate::fs::StdFs)));

    // Data in the active memtable, so the checkpoint's flush genuinely
    // installs a version (an empty flush never reaches `compaction_state`).
    tree.insert("pending-row", "v", 5_000);

    let table = {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table")
            .clone()
    };

    // Party 1 (this thread, standing in for a mid-install compaction): hold
    // `compaction_state` for the whole orchestration.
    let state_guard = tree.compaction_state.lock();

    // Party 2: the checkpoint. Its flush blocks on `compaction_state` (held
    // above). Pre-fix it blocked there while HOLDING the link window's write
    // half; the fix flushes before taking the window.
    let checkpoint = {
        let tree = Arc::clone(&tree);
        let dst = dir.path().join("checkpoint");
        std::thread::spawn(move || tree.create_checkpoint(&dst))
    };
    std::thread::sleep(Duration::from_millis(300));

    // Party 3: a heal patrol. It takes the table's heal lock and then enters
    // the mutation window (the link window's read half) — pre-fix it blocked
    // there while holding the heal lock.
    let patrol = {
        let tree = Arc::clone(&tree);
        std::thread::spawn(move || {
            patrol_scrub(&*tree, &PatrolScrubOptions::default().heal_in_place(true))
        })
    };
    std::thread::sleep(Duration::from_millis(300));

    // Party 1 now wants the heal lock, exactly like the tight-space slice loop
    // does while holding `compaction_state`. Pre-fix: the patrol holds it and
    // waits for the link window the checkpoint holds while the checkpoint
    // waits for our `compaction_state` — the cycle hangs all three.
    drop(table.heal_lock_arc().lock());
    drop(state_guard);

    let report = patrol.join().expect("patrol thread must not panic");
    assert!(report.is_ok(), "{report:?}");
    checkpoint
        .join()
        .expect("checkpoint thread must not panic")?;
    Ok(())
}

/// A checkpoint taken while a table carries a PENDING heal attestation (healed
/// bytes on disk, the live version still recording the pre-heal digest, and the
/// `.heal-attest` sidecar which the checkpoint does NOT copy) must not capture
/// that stale digest: the immutable checkpoint would fail integrity
/// verification forever with no marker to reconcile. The checkpoint reconciles
/// pending heals BEFORE snapshotting, so the captured version is self-consistent
/// (healed bytes under a refreshed digest).
#[cfg(feature = "page_ecc")]
#[test]
fn checkpoint_reconciles_a_pending_heal_before_snapshotting() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // Heal with a failing edit-log: the blocks are healed and the attestation
    // is written, but the manifest refresh fails, leaving a PENDING heal.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the failed refresh must leave a pending attestation",
    );

    // Snapshot while the heal is still pending.
    let dst = dir.path().join("checkpoint");
    tree.create_checkpoint(&dst)?;

    // The immutable checkpoint must verify clean: reconciling the pending heal
    // before the snapshot means the captured digest matches the linked (healed)
    // bytes (pre-fix it captured the stale pre-heal digest and failed forever).
    let checkpoint = open_ecc_tree(&dst);
    let integrity = crate::verify::verify_integrity(&checkpoint);
    assert!(
        integrity.is_ok(),
        "the checkpoint must not capture a table's stale pre-heal digest, got {:?}",
        integrity.errors,
    );
    Ok(())
}

/// A real-on-disk [`Fs`](crate::fs::Fs) (over `StdFs`) whose `exists` probe
/// FAILS for any `.heal-attest` sidecar, modelling a stat error on the
/// attestation probe. Every other operation delegates to `StdFs`.
mod exists_fail_fs {
    use crate::fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
    use crate::io;
    use std::path::Path;

    pub(super) struct ExistsFailFs;

    impl Fs for ExistsFailFs {
        fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
            StdFs.open(path, opts)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.create_dir_all(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
            StdFs.read_dir(path)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_file(path)
        }
        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            StdFs.rename(from, to)
        }
        fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
            StdFs.metadata(path)
        }
        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            StdFs.sync_directory(path)
        }
        fn exists(&self, path: &Path) -> io::Result<bool> {
            if path.to_string_lossy().ends_with(".heal-attest") {
                return Err(io::Error::other("injected attestation probe failure"));
            }
            StdFs.exists(path)
        }
        fn backend_id(&self) -> Option<u64> {
            StdFs.backend_id()
        }
        fn volume_id(&self, path: &Path) -> Option<u64> {
            StdFs.volume_id(path)
        }
    }
}

/// The checkpoint's pending-heal reconciliation probes each ECC table for a
/// `.heal-attest` sidecar. If that probe FAILS (an I/O error, not a clean
/// absent), treating it as "no pending heal" would let the checkpoint snapshot
/// the table's bytes under a possibly-stale digest with no marker to reconcile.
/// The probe must fail CLOSED: a probe error aborts the reconciliation, which
/// the checkpoint propagates (aborting the snapshot). Drives
/// [`reconcile_pending_heals`] directly — the exact step the checkpoint runs
/// before its link window — so the fault needs only the probe, not the
/// checkpoint's full filesystem surface.
#[test]
fn reconcile_pending_heals_aborts_when_the_attestation_probe_fails() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // A plain ECC table (no corruption needed): the reconcile still probes it
    // for a pending sidecar, and that probe is what fails here.
    let _ = write_ecc_sst_footered(dir.path());

    let tree = open_ecc_tree_on(
        dir.path(),
        std::sync::Arc::new(exists_fail_fs::ExistsFailFs),
    );

    let result = crate::scrub::reconcile_pending_heals(&tree);
    assert!(
        result.is_err(),
        "a failed attestation probe must abort the reconciliation, not silently \
         skip the table (pre-fix the probe error was swallowed as 'no pending heal'): \
         {result:?}",
    );
    Ok(())
}

/// The attributable heal path (the manifest digest matches the CURRENT pre-heal
/// bytes) is about to change bytes the manifest still matches, so its
/// crash-recovery attestation MUST be durable BEFORE the first block is mutated.
/// If the attestation cannot be persisted, the heal must ABORT (not proceed and
/// only log), because a crash after a corrected block syncs but before the
/// manifest refresh would leave healed bytes under the stale digest with no
/// marker, which fail-closed reconciliation rejects forever, permanently
/// stranding a table that was reconcilable a moment earlier. Leaving the block
/// corrupt keeps the table reconcilable; the next patrol retries.
#[test]
fn heal_in_place_aborts_when_the_attestation_cannot_be_persisted() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    // Seed the ATTRIBUTABLE reconcile scenario: rot a parity byte, then rebuild
    // the manifest over the rotted bytes so the manifest digest == the current
    // (pre-heal) file. `pre_heal_digest_matches` is then true and the heal would
    // change bytes the manifest currently matches (the only path that writes a
    // crash-recovery marker).
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;
    let before = std::fs::read(&sst_path)?;

    // Fault every open of the attestation sidecar so it can never be persisted.
    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    injector
        .arm(FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).on_path(".heal-attest"));

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    // The heal aborted: no block was mutated, so the file is byte-for-byte what
    // it was before the pass (pre-fix the heal proceeds and rewrites the block).
    assert_eq!(
        report.blocks_healed_in_place, 0,
        "the heal must not mutate any block when its attestation cannot be persisted: {report:?}",
    );
    let after = std::fs::read(&sst_path)?;
    assert_eq!(
        after, before,
        "the corrupt block must stay untouched so the table stays reconcilable",
    );
    // The skipped heal is surfaced as a finding, not silently swallowed.
    assert!(
        !report.is_ok(),
        "aborting the heal must surface a finding: {report:?}",
    );
    // A failed attestation write must not leave a partial marker behind.
    assert!(
        !heal_attest_path(&sst_path).exists(),
        "no partial attestation marker after a failed write",
    );
    Ok(())
}

/// A heal whose block WRITE landed but whose `sync_data` FAILED keeps its
/// attestation on purpose (the on-disk bytes may already differ from the
/// manifest digest, so a later patrol must still be able to attribute them).
/// That later patrol reads the corrected bytes back from the page cache and
/// finds the table clean — but refreshing the manifest digest then would
/// record a post-heal digest over bytes that were never synced: a power loss
/// discards the healed block while the manifest keeps the new digest, and the
/// table is permanently unreconcilable. The reconciliation must therefore sync
/// the SST data itself before refreshing, and keep reporting the table as
/// unreconciled when that sync fails.
#[test]
fn heal_reconcile_refuses_to_refresh_when_the_sst_data_cannot_be_synced() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());

    // Attributable heal scenario: rot a parity byte, rebuild the manifest over
    // the rotted bytes so the digest matches the CURRENT file.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;
    let manifest_digest = |tree: &crate::Tree| {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .map(crate::table::Table::checksum)
            .expect("the table is in the manifest")
    };

    // Heal with the SST's data sync faulted: the block write lands (so the
    // attestation is deliberately kept) while the bytes are never durable.
    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    let digest_before = manifest_digest(&tree);
    injector
        .arm(FaultRule::new(FaultOp::SyncData, Fault::Error(ErrorKind::Other)).on_path("tables"));
    let _ = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        heal_attest_path(&sst_path).exists(),
        "a write-attempted heal keeps its attestation for a later patrol",
    );

    // The follow-up patrol sees clean (page-cached) bytes and would refresh the
    // manifest digest through the marker — but the SST data still cannot be
    // synced, so the refresh must be refused.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        manifest_digest(&tree),
        digest_before,
        "the manifest digest must not be refreshed over bytes that were never \
         synced: a power loss would discard the healed block: {report:?}",
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "the marker survives so a later, syncable patrol can still reconcile",
    );
    Ok(())
}

/// The checkpoint's pre-window reconciliation only scans ECC tables, so a
/// pending `.heal-attest` marker it cannot consume (here: one on a non-ECC
/// table) whose digest is STALE — healed bytes not yet reconciled — must still
/// be caught by the post-link-window fail-closed guard rather than snapshot the
/// table under that stale digest with no marker. Models the residual race where
/// a genuine pending heal survives the pre-window reconcile. (A marker whose
/// digest already matches the manifest is obsolete and does NOT abort; see
/// [`abort_checkpoint_ignores_an_obsolete_marker_but_aborts_on_a_stale_digest`].)
#[test]
fn checkpoint_aborts_when_a_pending_marker_survives_the_pre_window_reconcile() -> crate::Result<()>
{
    use crate::AbstractTree;

    let dir = tempfile::tempdir()?;
    // A plain (non-ECC) tree: the pre-window reconcile scans only ECC tables,
    // so a marker planted here survives to the post-link-window guard.
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        unreachable!("standard tree configured");
    };
    for i in 0u64..4 {
        tree.insert(format!("key-{i:03}"), format!("v{i:03}"), i);
    }
    tree.flush_active_memtable(4)?;
    let sst_path = {
        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table");
        (*table.path).clone()
    };

    // Make the on-disk digest diverge from the manifest so the surviving marker
    // models a GENUINE pending heal (healed bytes the reconcile has not caught
    // up to), the case the post-window guard must abort on. Flip an interior
    // byte (length and trailer intact); the manifest still records the pre-flip
    // digest.
    let mut bytes = std::fs::read(&sst_path)?;
    if let Some(b) = bytes.get_mut(50) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;
    // Plant a pending marker the ECC-only pre-window reconcile will not consume.
    std::fs::write(heal_attest_path(&sst_path), b"pending marker")?;

    let dst = dir.path().join("checkpoint");
    let result = tree.create_checkpoint(&dst);
    assert!(
        result.is_err(),
        "a pending marker with a stale digest the pre-window reconcile cannot consume must \
         abort the checkpoint at the post-link-window guard: {result:?}",
    );
    Ok(())
}

/// The pre-heal digest probe (`live_region_checksum`, a SEQUENTIAL full-file
/// read) can fail transiently. Converting that I/O error into an ordinary
/// "digest does not match the manifest" would let the heal proceed writing
/// corrections with NO completed attestation: if the manifest legitimately
/// describes the degraded pre-heal bytes (a rebuild over a correctable fault),
/// the healed digest then differs from the manifest and reconciliation rejects
/// it forever with no marker. The probe failure must ABORT the heal before the
/// first write, exactly like a digest-prediction / attestation-persistence
/// failure. Faults only the sequential `Read` (the digest probe); block loads
/// use `read_at`, so the correctable fault is still discovered.
#[test]
fn heal_in_place_aborts_when_the_pre_heal_digest_probe_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_footered(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;
    let before = std::fs::read(&sst_path)?;

    // Fail the sequential digest read only (block loads use read_at).
    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    injector.arm(FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other)).on_path("tables"));

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "a failed pre-heal digest probe must abort before any block is mutated: {report:?}",
    );
    let after = std::fs::read(&sst_path)?;
    assert_eq!(
        after, before,
        "the corrupt block must stay untouched so the table stays reconcilable",
    );
    assert!(
        !report.is_ok(),
        "aborting the heal must surface a finding: {report:?}",
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

/// A reconciliation whose captured Table VIEW predates another patrol's
/// successful digest refresh must treat the already-reconciled file as
/// clean: the view's checksum snapshot is stale, but the CURRENT manifest
/// entry and the file agree, so there is nothing left to reconcile. Two
/// concurrent heal patrols hit exactly this interleaving — both capture
/// the same version before the per-table heal lock serializes them — and
/// on a footer-less table the loser has no attributable correction, so
/// the stale comparison surfaced as a spurious `ChecksumRefreshFailed`
/// on a healthy, already-reconciled table.
#[test]
fn checksum_refresh_is_idempotent_against_a_stale_table_view() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // Footer-less on purpose: without heal attribution the fail-closed
    // authoritative-content rule turns the stale mismatch into a finding.
    let (sst_path, block) = write_ecc_sst(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let tree = open_ecc_tree(dir.path());
    // Capture the table view BEFORE the heal, like a concurrently started
    // second patrol would.
    let stale_view = {
        let binding = tree.version_history.read().latest_version();
        binding
            .version
            .iter_tables()
            .next()
            .expect("one table")
            .clone()
    };

    // First patrol: heals the trailer in place and installs the refreshed
    // digest into the manifest.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted trailer is rebuilt in place: {report:?}",
    );
    assert!(report.is_ok(), "the first patrol reconciles: {report:?}");

    // Second patrol's reconcile step, still holding the pre-heal view: the
    // current manifest and the file already agree, so it must be a no-op.
    let finding = refresh_healed_checksum(&tree, &stale_view, false);
    assert!(
        finding.is_none(),
        "an already-reconciled file must not be reported: {finding:?}",
    );
    Ok(())
}

/// A PINNED file descriptor (a tree opened without an FD cache) cannot be
/// retargeted at the private copy the hard-link unshare produces: after
/// the copy + rename this Table would keep reading the DEAD inode forever
/// — reads, scrub probes, and digest checks all resolve through the
/// pinned handle while the manifest and live path refer to the healed
/// copy. The heal must REFUSE the detach instead: the blocked write-backs
/// surface as findings and every checkpoint link stays byte-identical to
/// what its manifest describes.
#[test]
fn heal_in_place_refuses_to_detach_under_a_pinned_descriptor() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // A second hard link stands in for a checkpoint's snapshot.
    let link = dir.path().join("checkpoint-link");
    std::fs::hard_link(&sst_path, &link)?;

    // No descriptor table: every Table pins one FD at recover time.
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
    .use_descriptor_table(None)
    .open()
    .expect("open pinned ecc tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    // The refused detach specifically: an UncorrectableBlock finding whose
    // reason names the unshare refusal, not any unrelated failure.
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ScrubError::UncorrectableBlock { reason, .. }
                if reason.starts_with("unshare hard-linked SST for heal: ")
        )),
        "a refused detach must surface as an unshare finding: {report:?}",
    );
    assert_eq!(
        report.blocks_healed_in_place, 0,
        "no block may be healed when the detach is refused: {report:?}",
    );
    assert_eq!(
        std::fs::read(&sst_path)?,
        std::fs::read(&link)?,
        "the live path must stay byte-identical to its checkpoint link (no detach)",
    );
    Ok(())
}

/// The pre-write ATTRIBUTION probe must compare against the CURRENT
/// manifest digest, not the caller's captured view: a reconciliation
/// running with a view captured before the manifest was re-recorded
/// (here: a manifest rebuilt over freshly rotted bytes after an earlier
/// heal already refreshed it) sees a stale checksum, marks a legitimate
/// heal unattributable, and the fail-closed rule then refuses to refresh
/// a footer-less table forever — even though the file's pre-write digest
/// matched the manifest exactly.
#[test]
fn heal_attribution_compares_against_the_current_manifest_digest() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // Footer-less: without attribution the fail-closed rule turns the
    // refusal into a permanent ChecksumRefreshFailed.
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Heal cycle one: rot, record the rotted digest, heal + refresh.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;
    let stale_view = {
        let tree = open_ecc_tree(dir.path());
        let stale = {
            let binding = tree.version_history.read().latest_version();
            binding
                .version
                .iter_tables()
                .next()
                .expect("one table")
                .clone()
        };
        let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
        assert!(report.is_ok(), "the first heal reconciles: {report:?}");
        stale
    };

    // Rot a SECOND fault — in a DIFFERENT block, so the rotted digest
    // differs from the first cycle's — and re-record the manifest over the
    // rotted bytes: the CURRENT manifest matches the file while the
    // captured view still carries the digest recorded before the first
    // heal.
    let second_block = {
        let keyed = stale_view
            .block_index
            .iter()
            .nth(1)
            .expect("the fixture writes several data blocks")
            .expect("block index entry decodes");
        crate::table::BlockHandle::new(keyed.offset(), keyed.size())
    };
    corrupt_parity_trailer_byte(&sst_path, &second_block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let tree = open_ecc_tree(dir.path());
    let report = scan_and_reconcile(
        &tree,
        &stale_view,
        &PatrolScrubOptions::default().heal_in_place(true),
    );
    assert!(
        report.blocks_healed_in_place >= 1,
        "the second fault is rebuilt in place: {report:?}",
    );
    assert!(
        report.is_ok(),
        "a heal whose pre-write digest matches the CURRENT manifest is \
         attributable and must reconcile: {report:?}",
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

/// A `{id}.heal-attest` sidecar left in `tables/` by a crashed heal refresh
/// must NOT break the next `Tree::open`: the table-folder scan skips it (it is
/// never a table file) instead of parsing its name as a `TableId` and returning
/// `Unrecoverable`. Deleting it here would forfeit the crashed-refresh recovery,
/// so the scan leaves it for the next scrub to consume.
#[test]
fn tree_open_skips_a_lingering_heal_attestation() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = {
        let crate::AnyTree::Standard(tree) = crate::Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?
        else {
            unreachable!("standard tree configured");
        };
        for i in 0u64..50 {
            tree.insert(format!("k{i:04}"), b"v", i);
        }
        tree.flush_active_memtable(50)?;
        let binding = tree.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };

    // A crashed refresh leaves this sidecar next to the SST.
    std::fs::write(heal_attest_path(&sst_path), b"pending attestation")?;

    // Reopen: the sidecar must be skipped, not parsed as a table id.
    let reopened = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();
    assert!(
        reopened.is_ok(),
        "a lingering heal-attest sidecar must not break open: {:?}",
        reopened.err(),
    );
    assert!(
        heal_attest_path(&sst_path).exists(),
        "open must leave the pending attestation for the next scrub to consume",
    );
    Ok(())
}
