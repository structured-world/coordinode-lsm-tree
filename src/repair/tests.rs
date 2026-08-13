use super::{
    compute_table_checksum, compute_table_checksum_with_overrides, highest_existing_version_id,
    quarantine_file, toc_may_hide_deletions, verify_keep_decision,
};
use crate::fs::StdFs;
use test_log::test;

/// `toc_may_hide_deletions` must PROPAGATE a transient open failure rather than
/// grade it `true` (fail closed): on a table `repair_with_salvage` already found
/// corrupt, a `true` verdict routes it to Quarantine — dropping the healthy
/// ranges block salvage could recover — when a retry of the probe could have
/// allowed that recovery. `true` is reserved for STRUCTURAL catalogue ambiguity.
/// A single Open fault on the probe reproduces the transient failure; the pre-fix
/// `let Ok else true` swallowed it and returned `true`.
#[test]
fn toc_may_hide_deletions_propagates_a_transient_open_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = crate::table::Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(crate::InternalValue::from_components(
        b"k".to_vec(),
        b"v".to_vec(),
        1,
        crate::ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted))
            .on_path("source")
            .once(),
    );
    let result = toc_may_hide_deletions(&fs, &source);
    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "a transient open failure must propagate, not grade the catalogue as hiding a \
         deletion section: {result:?}",
    );
    Ok(())
}

/// The mirror of [`toc_may_hide_deletions_propagates_a_transient_open_failure`]:
/// a PERSISTENT open failure (outside the transient allowlist) cannot be proven
/// harmless by a retry, so it fails closed (`Ok(true)`) — the corrupt table is
/// quarantined rather than salvaged into resurrected rows — instead of aborting
/// the whole repair.
#[test]
fn toc_may_hide_deletions_fails_closed_on_a_persistent_open_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = crate::table::Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(crate::InternalValue::from_components(
        b"k".to_vec(),
        b"v".to_vec(),
        1,
        crate::ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("source")
            .once(),
    );
    let result = toc_may_hide_deletions(&fs, &source);
    assert!(
        matches!(result, Ok(true)),
        "a persistent open failure must fail closed (quarantine), not propagate: {result:?}",
    );
    Ok(())
}

/// `compute_table_checksum_with_overrides` splices corrections chunk by chunk
/// (256 KiB). An override that does NOT overlap the current chunk must be
/// skipped cleanly. Before the overlap guard was hoisted above the bound
/// subtractions, a multi-chunk file underflowed an unsigned difference (e.g.
/// `hi - chunk_start` for an override that ends before the chunk starts) and
/// panicked in debug builds while predicting the post-heal digest. The result
/// must equal a manual splice.
#[test]
fn checksum_with_overrides_skips_non_overlapping_overrides_across_chunks() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions};
    use std::io::Write;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("multi-chunk.sst");

    // Three 256 KiB chunks plus a tail, deterministic bytes.
    let len: usize = 3 * 256 * 1024 + 777;
    let mut data: Vec<u8> = (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();

    let fs = StdFs;
    {
        let mut f = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        f.write_all(&data)?;
    }

    // One override entirely within the FIRST chunk: chunks 1 and 2 do not
    // overlap it, which is exactly the non-overlap path that used to underflow.
    let ov_off: usize = 100;
    let ov_bytes = vec![0xABu8; 4096];
    let overrides = vec![(ov_off as u64, ov_bytes.clone())];

    // Manual splice for the expected digest (mirrors the streaming hasher the
    // implementation uses).
    if let Some(slot) = data.get_mut(ov_off..ov_off + ov_bytes.len()) {
        slot.copy_from_slice(&ov_bytes);
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    hasher.update(&data);
    let expected = hasher.digest128();

    let got = compute_table_checksum_with_overrides(&fs, &path, 0, &overrides)?;
    assert_eq!(
        got, expected,
        "the spliced digest must match a manual splice and must not panic on \
         chunks the override does not overlap",
    );
    Ok(())
}

/// `quarantine_file` must fsync BOTH affected directories (the source's parent
/// and the `repair-quarantine/` destination) before returning success: a
/// rename is durable only once both directory entries are on disk. Without it a
/// power loss after repair returns can lose the destination entry or restore
/// the source under `tables/`, and the next open's orphan cleanup then deletes
/// the only copy meant for manual recovery. Fault-inject the
/// destination-directory fsync: a build that never syncs the directory never
/// triggers the fault and wrongly reports the move durable.
#[test]
fn quarantine_file_syncs_the_affected_directories() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let src = tables.join("junk-name");
    std::fs::write(&src, b"orphan")?;

    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path("repair-quarantine"),
    );

    assert!(
        quarantine_file(&fs, &tables, &src, "junk-name", SyncMode::Full).is_err(),
        "the destination-directory fsync fault must surface",
    );

    // The SOURCE directory entry must be synced too: a power loss can otherwise
    // restore the file under `tables/`, where the next open's orphan cleanup
    // deletes it. Arm the fault on the source parent so this case fails
    // independently if the src.parent() sync is dropped.
    let src2 = tables.join("junk-name-2");
    std::fs::write(&src2, b"orphan")?;
    let fs2 = FaultFs::new(StdFs);
    fs2.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).on_path("tables"),
    );
    assert!(
        quarantine_file(&fs2, &tables, &src2, "junk-name-2", SyncMode::Full).is_err(),
        "the source-directory fsync fault must surface",
    );
    Ok(())
}

/// Creating the `repair-quarantine/` directory adds its entry to the PARENT
/// directory; that entry must be fsynced before the move so a power loss after
/// repair returns cannot drop the whole quarantine directory (and the only
/// preserved copy of the original). The parent sync runs right after the
/// directory is created — BEFORE the rename — so faulting the first directory
/// sync leaves the source UNMOVED. Without the parent sync the first sync is
/// the post-rename source sync, by which point the file is already moved: the
/// surviving-source assertion then fails, proving the fresh directory's parent
/// entry was never made durable.
#[test]
fn quarantine_file_syncs_the_freshly_created_quarantine_parent() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let src = tables.join("junk-name");
    std::fs::write(&src, b"orphan")?;

    // Fail the FIRST directory sync. The quarantine directory does not exist
    // yet, so the fix's parent sync (post-create, pre-rename) is that first
    // sync — its failure aborts before the rename moves the source.
    let fs = FaultFs::new(StdFs);
    fs.injector()
        .arm(FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).once());

    assert!(
        quarantine_file(&fs, &tables, &src, "junk-name", SyncMode::Full).is_err(),
        "the quarantine-parent fsync fault must surface",
    );
    assert!(
        std::fs::metadata(&src).is_ok(),
        "the pre-rename parent-sync failure must leave the source in place",
    );
    Ok(())
}

/// The parent sync must run on EVERY quarantine, not only when the directory is
/// freshly created. A previous repair that created `repair-quarantine` but
/// crashed before syncing its parent leaves that directory entry non-durable; a
/// retry that skips the sync (because the directory now exists) moves the only
/// preserved source in without ever making the parent durable, so a power loss
/// loses the whole quarantine directory. With an already-present quarantine
/// directory, faulting the first sync must still abort BEFORE the rename —
/// leaving the source in place — which proves the parent is synced every time.
#[test]
fn quarantine_file_syncs_the_parent_on_a_retry_with_a_preexisting_dir() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    // A prior repair already created the quarantine directory.
    std::fs::create_dir_all(dir.path().join("repair-quarantine"))?;
    let src = tables.join("junk-name");
    std::fs::write(&src, b"orphan")?;

    // Fail the FIRST directory sync. Because the parent sync is unconditional, it
    // is that first sync (post-create, pre-rename) even though the directory
    // already exists, so its failure aborts before the rename moves the source.
    let fs = FaultFs::new(StdFs);
    fs.injector()
        .arm(FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).once());

    assert!(
        quarantine_file(&fs, &tables, &src, "junk-name", SyncMode::Full).is_err(),
        "the quarantine-parent fsync fault must surface on a retry",
    );
    assert!(
        std::fs::metadata(&src).is_ok(),
        "the pre-rename parent-sync failure must leave the source in place on a retry",
    );
    Ok(())
}

/// A POST-rename directory-sync failure must ROLL THE RENAME BACK: the move is
/// not durably committed, so leaving the source in quarantine lets a retry
/// install a manifest omitting it; after which a power loss that rolls the
/// un-synced rename back resurrects the source as an orphan the next open
/// deletes, losing the only recovery copy. The source must return to `tables/`
/// so a retry can still find and re-quarantine it durably. The quarantine-dir
/// sync (which fires only AFTER the rename) is faulted, so the failure lands
/// with the source already moved.
#[test]
fn quarantine_file_rolls_the_rename_back_on_a_post_move_sync_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let src = tables.join("junk-name");
    std::fs::write(&src, b"orphan")?;

    // Fault the quarantine-directory sync only: it runs AFTER the rename (the
    // pre-rename parent sync targets `dir.path()`, which does not contain
    // "repair-quarantine"), so the failure lands with the source already moved.
    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path("repair-quarantine"),
    );

    assert!(
        quarantine_file(&fs, &tables, &src, "junk-name", SyncMode::Full).is_err(),
        "the post-move directory-sync fault must surface",
    );
    assert!(
        std::fs::metadata(&src).is_ok(),
        "the rename must be rolled back so the source stays under tables/ for a retry",
    );
    assert!(
        std::fs::metadata(dir.path().join("repair-quarantine").join("junk-name")).is_err(),
        "the half-committed move must not linger in quarantine",
    );
    Ok(())
}

/// A second repair of the same table must NOT overwrite an earlier quarantine
/// copy: `rename` replaces the destination on Unix, so a fixed
/// `repair-quarantine/{id}` name would destroy the only copy of the previous
/// corrupt original kept for manual recovery. The move must land on a distinct,
/// create-new name instead.
#[test]
fn quarantine_file_preserves_an_earlier_copy_of_the_same_table() -> crate::Result<()> {
    use crate::fs::SyncMode;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;

    let src1 = tables.join("7");
    std::fs::write(&src1, b"first-copy")?;
    let dest1 = quarantine_file(&StdFs, &tables, &src1, "7", SyncMode::Normal)?;
    assert_eq!(std::fs::read(&dest1)?, b"first-copy");

    // A later repair of the SAME table id produces a new corrupt file at the
    // same name; quarantining it must preserve the earlier copy.
    let src2 = tables.join("7");
    std::fs::write(&src2, b"second-copy")?;
    let dest2 = quarantine_file(&StdFs, &tables, &src2, "7", SyncMode::Normal)?;

    assert_ne!(
        dest1, dest2,
        "the second quarantine must land on a distinct name",
    );
    assert_eq!(
        std::fs::read(&dest1)?,
        b"first-copy",
        "the earlier quarantine copy must survive the second repair",
    );
    assert_eq!(std::fs::read(&dest2)?, b"second-copy");
    Ok(())
}

#[test]
fn compute_table_checksum_matches_oneshot_xxh3() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("000007");
    // Larger than the 256 KiB read buffer so the chunked read loop is
    // exercised across multiple iterations.
    let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &payload)?;

    let got = compute_table_checksum(&StdFs, &path)?;
    let expected = xxhash_rust::xxh3::xxh3_128(&payload);
    assert_eq!(
        got, expected,
        "streamed digest must equal the one-shot xxh3-128 digest",
    );
    Ok(())
}

#[test]
fn highest_existing_version_id_picks_the_max_and_ignores_non_versions() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    for name in ["v2", "v10", "v3", "current", "vNaN", "notaversion"] {
        std::fs::write(dir.path().join(name), b"x")?;
    }
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, Some(10));
    Ok(())
}

#[test]
fn highest_existing_version_id_none_when_no_versions_present() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("current"), b"x")?;
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, None);
    Ok(())
}

/// `repair_with_salvage` on an SST whose ONLY data block is corrupt: whole-file
/// recovery still succeeds (the data section is read lazily) but verification
/// fails, and block-salvage finds nothing recoverable, so the table is reported
/// unreadable rather than kept as one that errors on every read.
#[test]
fn repair_with_salvage_reports_a_sole_corrupt_block_as_unsalvageable() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A handful of short keys fit in a single data block: no second block for
    // salvage to fall back on.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Resolve the sole data block's offset from the intact index, then flip a
    // byte just past its header so the block fails its checksum. The container,
    // index and meta stay intact, so whole-file recovery still opens it (data is
    // read lazily) and only verification trips.
    let offset = sole_data_block_offset(&recover_table(sst.clone(), &fs)?);
    let flip = usize::try_from(offset).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "the sole block is corrupt: nothing to salvage",
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    assert_eq!(
        report.unreadable, 1,
        "the unsalvageable SST is reported: {:?}",
        report.unreadable_files,
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("nothing salvageable"),
        "the reason names the empty salvage, got: {reason}",
    );
    Ok(())
}

/// A PERSISTENT read failure while hashing the whole file (a bad data sector)
/// must not doom a salvageable table: pre-fix, repair recorded it unreadable
/// BEFORE block-salvage could run, and the next open's orphan cleanup then
/// deleted its intact blocks. With `repair_with_salvage(true)` the whole-file
/// hash failure is folded into the recovery path, so block-salvage recovers the
/// readable blocks. The fault fires once on the preliminary hash of the
/// original (block-salvage reads the quarantined copy and the reopened salvaged
/// copy, both unaffected).
#[test]
fn repair_with_salvage_recovers_a_table_whose_whole_file_hash_faults() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");

    // Several small blocks: block-salvage can recover them all (the fault is on
    // the whole-file hash, not on any block read).
    {
        let build_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&build_fs))?.use_data_block_size(128);
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Fault the FIRST streaming read of the original SST (the preliminary
    // whole-file hash) with a persistent `Other`/EIO. `.once()` leaves the
    // block-salvage reads of the quarantined copy — and the reopen-hash of the
    // clean salvaged copy — unfaulted, so recovery proceeds.
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    injector.arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    injector.clear();

    assert_eq!(
        report.unreadable, 0,
        "a whole-file hash fault must not record the table unreadable: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.salvaged, 1,
        "the table is recovered through block-salvage"
    );
    assert_eq!(
        report.recovered, 1,
        "the salvaged table joins the rebuilt manifest"
    );
    Ok(())
}

/// A TRANSIENT I/O failure during block-salvage must not commit a manifest that
/// OMITS the table: the original is already in `repair-quarantine`, so a retry
/// (no longer finding it under `tables/`) would never rediscover it and the SST
/// would be permanently lost. Repair must instead RESTORE the quarantined
/// original to its path and abort, so the operator can retry. Fault the salvage's
/// open of the quarantined source; pre-fix, repair recorded it unreadable and
/// committed a manifest without the table.
#[test]
fn repair_with_salvage_restores_the_original_on_a_transient_salvage_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");

    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();

    // A single-block SST whose sole data block is corrupt: repair routes it to
    // salvage (verdict Corrupt).
    {
        let build_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&build_fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
        let offset = sole_data_block_offset(&recover_table(sst.clone(), &build_fs)?);
        let flip = usize::try_from(offset).unwrap_or(0) + 16;
        let mut bytes = std::fs::read(&sst)?;
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
        std::fs::write(&sst, &bytes)?;
    }

    // Fault the salvage's open of the quarantined source (the first open of a
    // `repair-quarantine/` path) with an interrupted-syscall error (the
    // unambiguously transient kind): try_salvage_table then fails transiently.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted))
            .on_path("repair-quarantine")
            .once(),
    );

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true);
    injector.clear();

    assert!(
        result.is_err(),
        "a transient salvage failure must abort the repair, not commit without the table: \
         {result:?}",
    );
    assert!(
        sst.exists(),
        "the quarantined original must be restored to its path so a retry can recover it",
    );
    let quarantine = dir.path().join("repair-quarantine");
    let stranded = quarantine.exists()
        && std::fs::read_dir(&quarantine)?
            .find_map(Result::ok)
            .is_some();
    assert!(
        !stranded,
        "the original must be moved OUT of quarantine, not stranded there",
    );
    Ok(())
}

/// `repair_with_salvage` on an SST that carries range tombstones and a corrupt
/// data block: whole-file recovery opens it (data is read lazily) but
/// verification trips on the corrupt block, and block-salvage refuses it because
/// it cannot re-emit the range tombstones, so the table is reported unreadable.
#[test]
fn repair_with_salvage_reports_a_range_tombstone_sst_as_unsalvageable() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt the sole data block (offset from the intact index) so whole-file
    // recovery opens it but verification fails, driving repair into salvage.
    let offset = sole_data_block_offset(&recover_table(sst.clone(), &fs)?);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(usize::try_from(offset).unwrap_or(0) + 16) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "salvage refuses an SST with range tombstones",
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("salvage failed") && reason.contains("range tombstones"),
        "the reason names the failed salvage, got: {reason}",
    );
    Ok(())
}

/// A TRANSIENT read failure while HASHING an SST during repair must not lose the
/// table: recording it unreadable commits a manifest that omits the still-in-
/// place file, which the next open's orphan cleanup then deletes. Repair must
/// propagate the transient I/O and abort so a retry re-reads the table. Fault the
/// first open of the file (the checksum hash's open); targeting the op by path,
/// not a per-file open COUNT, keeps the test platform-independent (the count
/// differs across OSes).
#[test]
fn repair_aborts_on_a_transient_checksum_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");

    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();

    {
        let build_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&build_fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // compute_table_checksum is the FIRST thing the per-table loop does, and it
    // hashes the file with sequential reads. Fault the first READ under `tables/`
    // with an interrupted-syscall error (the unambiguously transient kind): the
    // directory scan does not read file bytes, so this lands on the hash's read.
    // Matching by the `tables` path component (not `tables/0`) and by the op (not
    // an open COUNT) keeps it platform-independent.
    injector.arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Interrupted))
            .on_path("tables")
            .once(),
    );

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true);
    injector.clear();

    assert!(
        result.is_err(),
        "a transient checksum-hash failure must abort the repair, not record the table \
         unreadable and commit without it: {result:?}",
    );
    assert!(
        sst.exists(),
        "the healthy SST must be left in place for a retry",
    );
    Ok(())
}

/// `repair_with_salvage` on a columnar SST whose delete-bitmap AND sole data
/// block are both corrupt: whole-file recovery refuses it (the corrupt bitmap
/// would resurrect deleted rows) and automated block-salvage fails closed on
/// the unreadable bitmap before even walking the blocks, so the table is
/// reported unreadable rather than half-recovered.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_reports_a_corrupt_bitmap_and_block_sst_as_unsalvageable() -> crate::Result<()>
{
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A small columnar SST (single data block) carrying a delete-bitmap.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_columnar(true)
            .use_zone_map(true)
            .delete_strategy(DeleteStrategy::MergeOnRead);
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        for pos in [2u32, 6] {
            w.delete_bitmap_mut().insert(pos);
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Resolve the sole data block's offset from the intact index before any
    // corruption shifts nothing (the flip is in place, lengths are unchanged).
    let block_offset = sole_data_block_offset(&recover_table(sst.clone(), &fs)?);
    let bitmap = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("the SST must carry a delete_bitmap section");
        };
        usize::try_from(entry.pos() + entry.len() / 2).unwrap_or(0)
    };

    // Corrupt the sole data block (so salvage recovers nothing) and the bitmap
    // (so whole-file recovery refuses to open it at all).
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(usize::try_from(block_offset).unwrap_or(0) + 16) {
        *b ^= 0xFF;
    }
    if let Some(b) = bytes.get_mut(bitmap) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "the bitmap is unreadable: automated salvage fails closed"
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("salvage failed"),
        "the reason names the failed salvage, got: {reason}",
    );
    Ok(())
}

/// `repair_with_salvage` must not accept a table whose ECC descriptor this
/// build cannot interpret: the out-of-band verify skips the SST-block
/// sections for such a table (their parity-trailer length is underivable), so
/// its report carries a WARNING and the on-disk bytes are effectively
/// unchecked. A gate that only checks `is_ok()` stamps those unchecked bytes
/// into the rebuilt manifest; the repair must instead salvage the table
/// (re-encode under a recognized descriptor) so the result is verifiable.
#[test]
fn repair_with_salvage_rewrites_an_unrecognized_ecc_sst() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    let n = 200u32;
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..n {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Forge an UNRECOGNIZED ECC descriptor into both meta blocks (`meta` and
    // its `meta_mid` mirror) and re-stamp their checksums. Meta blocks are
    // written with restart interval 1 (no prefix truncation), so the key
    // appears verbatim in the payload, followed by a one-byte value length
    // (4) and the 4-byte descriptor. `[0, 8, 2, 1]` is a non-canonical "off"
    // (junk reserved bytes) that decodes to `ecc_unrecognized = true`: reads
    // still work (payload framed by data_length), but the out-of-band verify
    // cannot size the parity trailers, so the SST-block sections are skipped
    // with a warning — the table's on-disk bytes are effectively UNCHECKED.
    forge_unrecognized_ecc_descriptor(&sst)?;
    // Precondition: the forged table opens and is flagged unrecognized.
    assert!(
        recover_table(sst.clone(), &fs)?.metadata.ecc_unrecognized,
        "the forged descriptor must flag the table as unrecognized-ECC",
    );

    // Salvage-mode repair must NOT stamp a table whose block sections could
    // not be verified into the rebuilt manifest as-is: the warning-bearing
    // verify report means the bytes are unchecked, so the table is salvaged
    // (re-encoded under a recognized descriptor) instead.
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "an unverifiable unrecognized-ECC table is rewritten, not accepted \
         as-is: {report:?}",
    );
    assert!(
        report.unreadable_files.is_empty(),
        "every row is recoverable: {:?}",
        report.unreadable_files,
    );

    // The rewritten table carries a recognized descriptor and every row.
    let table = recover_table(sst, &fs)?;
    assert!(
        !table.metadata.ecc_unrecognized,
        "the salvaged copy is re-stamped with a recognized descriptor",
    );
    assert_eq!(table.metadata.item_count, u64::from(n));
    Ok(())
}

/// Recovers the SST at `path` as a `Table`, stamping the open with the file's
/// current digest.
fn recover_table(
    path: std::path::PathBuf,
    fs: &std::sync::Arc<dyn crate::fs::Fs>,
) -> crate::Result<crate::table::Table> {
    use std::sync::Arc;
    let checksum = crate::Checksum::from_raw(super::compute_table_checksum(&**fs, &path)?);
    crate::table::Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )
}

/// The offset of the SOLE data block in `table`, panicking if there is not
/// exactly one. The salvage / repair tests build single-data-block SSTs, so
/// this collapses the repeated "collect handles, assert one, take its offset".
fn sole_data_block_offset(table: &crate::table::Table) -> u64 {
    let offsets: alloc::vec::Vec<u64> = table
        .data_block_handles()
        .filter_map(Result::ok)
        .map(|kh| *kh.as_ref().offset())
        .collect();
    let [only] = offsets.as_slice() else {
        panic!("expected a single data block, got {offsets:?}");
    };
    *only
}

/// A TRANSIENT read error while block-verifying a healthy table must abort the
/// repair, not be laundered into a corruption verdict: routing it through
/// salvage would drop the "unreadable" block and install a partial replacement,
/// turning a retryable I/O failure into permanent missing data. The intact
/// block stays on disk, so the operator retries and the next attempt reads it.
#[test]
fn repair_with_salvage_propagates_a_transient_verify_io_error() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A single-data-block SST whose bytes are entirely intact.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Resolve the sole data block's offset, then fault ONLY the positioned read
    // at that offset, exactly once. The raw-checksum verify walk streams the
    // file (sequential `Read`), and whole-file recovery is lazy on the data
    // section, so neither trips: the first positioned read at this offset is the
    // block-verify DECODE-load, which then surfaces a transient `Io` error.
    // `Interrupted` is the genuine transient kind (the `is_transient_io`
    // allowlist): a persistent `Other` would instead grade as corruption and
    // salvage, which is the sibling `is_corruption_routes_a_persistent_io...` case.
    let offset = sole_data_block_offset(&recover_table(sst.clone(), &fs)?);
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted))
            .at_offset(offset)
            .once(),
    );

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fault))
    .repair_with_salvage(true);
    injector.clear();

    assert!(
        result.is_err(),
        "a transient read error during block verification must abort the repair \
         (not salvage a healthy block into missing data), got {result:?}",
    );
    // The intact original is untouched: the abort happens before any quarantine
    // move, so the operator can retry.
    assert!(
        fs.metadata(&sst).is_ok(),
        "the original file stays in place after the aborted repair",
    );
    Ok(())
}

/// A pending `{id}.heal-attest` sidecar must be PRESERVED by repair, not
/// quarantined: `Tree::open` recognizes and keeps it (the next scrub reconciles
/// a crashed digest refresh through it). Repair quarantining it would strand the
/// healed table under its stale pre-heal digest if the manifest rebuild later
/// failed before committing. The sidecar must still sit next to its SST after a
/// successful repair.
#[test]
fn repair_preserves_a_pending_heal_attestation_sidecar() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let sidecar = tables.join("0.heal-attest");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // A pending heal marker left next to the SST (its bytes are opaque to the
    // repair scan; only the file name matters here).
    std::fs::write(&sidecar, b"pending-heal-marker")?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(report.recovered, 1, "the table is recovered: {report:?}");
    assert!(
        sidecar.exists(),
        "the heal-attest sidecar stays next to its SST (not quarantined)",
    );
    Ok(())
}

/// A file whose name only LOOKS like a heal-temp — `{id}.healtmp-{non-numeric}`
/// (e.g. `5.healtmp-backup`) — must NOT be skipped as an owned artifact: recovery
/// owns only `{id}.healtmp-{numeric}`, so leaving it in place makes the next
/// `Tree::open` reject its non-numeric name instead of sweeping it, leaving the
/// repaired database unopenable. Repair must quarantine it like any foreign file.
#[test]
fn repair_quarantines_a_foreign_healtmp_suffix() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    // Prefix parses as a table id, but the sequence does not parse as u64.
    let foreign = tables.join("5.healtmp-backup");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    std::fs::write(&foreign, b"not a real heal temp")?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(
        report.recovered, 1,
        "the real table is recovered: {report:?}"
    );
    assert!(
        !foreign.exists(),
        "the foreign .healtmp- file is quarantined out of tables/, not left to \
         break the next open",
    );
    Ok(())
}

/// A PERSISTENT read failure DURING the block-verify walk (not the decode-load)
/// is graded as corruption, not an abort: `verify_sst_file_with_context` reports
/// it as `SstFileUnreadable` / `DataReadError`, and a retry can never fix a bad
/// sector, so aborting the whole repair forever would strand every healthy
/// sibling table. The corrupt table is routed to salvage instead. (Only the
/// transient allowlist — `Interrupted` / `WouldBlock` — aborts for a retry, but
/// those kinds are absorbed by the read layer's own EINTR retry before reaching
/// this gate, so the abort arm is defensive.) The table is recovered on a clean
/// fs, then the walk runs on a fs whose read faults once, so only the walk trips.
#[test]
fn verify_keep_decision_grades_a_persistent_walk_io_error_as_corruption() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let clean: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&clean))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    let table = recover_table(sst.clone(), &clean)?;

    // A single persistent `Other` fault on the walk read: the verdict must NOT
    // abort (that is the transient contract), so `verify_keep_decision` returns a
    // decision rather than propagating the error.
    let fault = FaultFs::new(StdFs);
    fault
        .injector()
        .arm(FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other)).once());
    let faulting: Arc<dyn crate::fs::Fs> = Arc::new(fault);

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let decision = verify_keep_decision(&config, &faulting, &sst, &table);
    assert!(
        decision.is_ok(),
        "a persistent walk read error must be graded as corruption (a decision), not \
         abort the whole repair, got {decision:?}",
    );
    Ok(())
}

/// PARITY-ONLY rot (every payload checksum still clean) on a table salvage
/// cannot faithfully re-emit (range tombstones) must also KEEP the table:
/// the data is fully readable, only its recovery margin is degraded, and
/// quarantining it through a salvage that is guaranteed to refuse would
/// throw the table away over dead parity.
#[cfg(feature = "page_ecc")]
#[test]
fn repair_with_salvage_keeps_a_parity_rotted_range_tombstone_sst() -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::table::block::{EccParams, Header};
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_ecc(Some(EccParams::try_new(4, 2)?));
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Rot one byte of the sole data block's PARITY trailer: the payload
    // checksum still verifies, so the out-of-band walk reports only an
    // EccParityMismatch — the data itself is untouched.
    let block_off = usize::try_from(sole_data_block_offset(&recover_table(sst.clone(), &fs)?))
        .unwrap_or(usize::MAX);
    let mut bytes = std::fs::read(&sst)?;
    let Some(mut cursor) = bytes.get(block_off..) else {
        panic!("data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos =
        block_off + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert!(
        report.unreadable_files.is_empty(),
        "a readable table must not be dropped over parity-only rot: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.recovered, 1,
        "the range-tombstone table joins the rebuilt manifest as-is: {report:?}",
    );
    assert_eq!(
        report.salvaged, 0,
        "salvage is never attempted when it cannot re-emit the range tombstones",
    );
    Ok(())
}

/// The unrecognized-ECC degraded grade is different from parity-only rot: the
/// out-of-band walk SKIPPED the SST-block sections entirely (their trailer
/// length is underivable), so nothing about the data was verified. The
/// range-tombstone keep-guard must therefore NOT keep such a table blindly —
/// a corrupt lazy data block would ride into the rebuilt manifest. The table
/// is verified through handle-based reads instead (they frame the payload by
/// `data_length` and checksum-verify it regardless of the descriptor); a
/// corrupt block then routes to salvage, which refuses range tombstones, so
/// the table is reported unreadable rather than silently kept.
#[test]
fn repair_with_salvage_rejects_a_corrupt_unrecognized_ecc_tombstone_sst() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Forge the unrecognized descriptor (the out-of-band walk then skips the
    // data section) AND corrupt the sole data block's payload.
    forge_unrecognized_ecc_descriptor(&sst)?;
    let block_off = usize::try_from(sole_data_block_offset(&recover_table(sst.clone(), &fs)?))
        .unwrap_or(usize::MAX);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(block_off + 40) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a corrupt table whose sections the walk could not scan must not be \
         kept over the range-tombstone escape hatch: {report:?}",
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("range tombstones"),
        "the reason names the refused range-tombstone salvage, got: {reason}",
    );
    Ok(())
}

/// An unrecognized-ECC range-tombstone table can be neither verified in full
/// (the block walk skips its sections; every lazy side structure would need
/// its own handle-based check) nor faithfully salvaged (range tombstones are
/// not re-emittable) — even a HEALTHY one is therefore QUARANTINED for
/// manual recovery rather than riding unverified into the rebuilt manifest,
/// and the quarantine protects it from the orphan cleanup a later open runs.
#[test]
fn repair_with_salvage_quarantines_an_unrecognized_ecc_tombstone_sst() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    forge_unrecognized_ecc_descriptor(&sst)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "an unverifiable table never joins the rebuilt manifest: {report:?}",
    );
    assert_eq!(
        report.salvaged, 0,
        "salvage cannot re-emit range tombstones"
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("quarantined") && reason.contains("recompact"),
        "the reason names the quarantine and the recovery path, got: {reason}",
    );
    // The original bytes survive in the quarantine (a later open's orphan
    // cleanup would delete an unquarantined file the manifest ignores).
    let quarantined = dir.path().join("repair-quarantine").join("0");
    assert!(
        fs.metadata(&quarantined).is_ok(),
        "the original file is preserved in the quarantine",
    );
    Ok(())
}

/// Repair's out-of-band block verify must apply the SAME caller-known-id
/// cross-check as recovery when it reads the meta block for the ECC
/// descriptor: a checksum-clean TAIL meta whose `table_id` AND ECC descriptor
/// were forged (MID intact) is rejected by recovery's id check and falls back
/// to the intact MID — but a verify probe that skips the id check for
/// unencrypted reads accepts the forged tail, grades the healthy table
/// degraded-UNSCANNED off the forged descriptor, and (for a range-tombstone
/// SST salvage cannot re-emit) quarantines a perfectly healthy table.
#[test]
fn repair_with_salvage_keeps_a_healthy_rt_sst_with_a_forged_tail_meta() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A HEALTHY range-tombstone SST under id 7 (salvage cannot re-emit range
    // tombstones, so a wrong degraded-unscanned verdict is terminal for it).
    {
        let mut w = Writer::new(sst.clone(), 7, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // Forge ONLY the tail meta (id 7 → 99 AND an unrecognized ECC
    // descriptor); the MID mirror keeps the true id and descriptor.
    forge_tail_meta_table_id(&sst, Some(99), Some([0u8, 8, 2, 1]))?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the healthy table joins the rebuilt manifest via the intact MID \
         meta: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.unreadable, 0,
        "no quarantine for a healthy table whose forged tail the id \
         cross-check rejects: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// A tail meta whose table id is CORRECT but whose ECC descriptor alone was
/// forged (checksum restamped) passes the verify probe's id cross-check, so
/// the probe must not stop there: it has to fall back to the intact MID
/// mirror before treating the table as unscanned. Without the fallback a
/// healthy range-tombstone SST is graded degraded-unscanned off the forged
/// descriptor and quarantined even though the MID copy carries the valid one.
#[test]
fn repair_with_salvage_keeps_a_healthy_rt_sst_with_a_forged_tail_descriptor_only()
-> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A HEALTHY range-tombstone SST under id 7 (salvage cannot re-emit range
    // tombstones, so a wrong degraded-unscanned verdict is terminal for it).
    {
        let mut w = Writer::new(sst.clone(), 7, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // Forge ONLY the tail meta's ECC descriptor — its table id stays the
    // TRUE 7, so the id cross-check passes; the MID mirror keeps the valid
    // descriptor.
    forge_tail_meta_table_id(&sst, None, Some([0u8, 8, 2, 1]))?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the healthy table joins the rebuilt manifest via the MID mirror's \
         valid descriptor: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.unreadable, 0,
        "no quarantine for a healthy table whose forged tail descriptor the \
         MID fallback overrides: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// A FAILED quarantine must abort the repair: the rebuilt manifest omits the
/// unverifiable table, so a later `Tree::open` orphan-cleans the still-in-place
/// original — installing that manifest after the move failed would let the
/// next open DELETE the only copy instead of preserving it for manual
/// recovery. The repair must propagate the quarantine error and leave no
/// rebuilt manifest behind.
#[test]
fn repair_aborts_when_the_quarantine_move_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // An unverifiable range-tombstone SST: the repair routes it to quarantine.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    forge_unrecognized_ecc_descriptor(&sst)?;

    // Fail the quarantine move (rename matched against its destination).
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    injector.arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
            .on_path("repair-quarantine")
            .once(),
    );

    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fault))
    .repair_with_salvage(true);
    injector.clear();

    assert!(
        result.is_err(),
        "a failed quarantine must abort the repair (a rebuilt manifest \
         omitting the still-in-place file would let the next open delete the \
         only copy), got {result:?}",
    );
    // The original is still where it was — nothing moved, nothing lost.
    assert!(
        fs.metadata(&sst).is_ok(),
        "the original file stays in place after the aborted repair",
    );
    Ok(())
}

/// The escape-hatch fallback scrub must be trusted only when it saw EVERY
/// block: `scrub_data_blocks` records a block-index walk failure in `errors`
/// WITHOUT counting an uncorrectable block, so a gate that only checks
/// `is_ok()` treats a table whose data blocks were never enumerated (a
/// corrupt partitioned-index leaf) as verified clean and keeps it.
#[test]
fn repair_with_salvage_rejects_an_unrecognized_ecc_tombstone_sst_with_a_corrupt_index()
-> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        // Partitioned index: its leaf blocks load lazily, so a corrupt leaf
        // survives recovery and only surfaces when the fallback scrub walks
        // the block index.
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_partitioned_index();
        for i in 0..200u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Forge the unrecognized descriptor (out-of-band walk skips the data AND
    // index sections) and corrupt an index leaf so the handle-based fallback
    // cannot enumerate the data blocks.
    forge_unrecognized_ecc_descriptor(&sst)?;
    let (index_pos, index_len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"index") else {
            panic!("a partitioned-index SST must carry an index section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(index_pos + index_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a table whose data blocks the fallback scrub could not enumerate \
         must not be kept: {report:?}",
    );
    assert_eq!(
        report.unreadable_files.len(),
        1,
        "the unverifiable table is reported unreadable: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// An unrecognized ECC descriptor combined with parity-only errors in the
/// still-walked self-describing meta blocks must grade as UNSCANNED, not
/// merely degraded: the SST data/index sections were skipped entirely, so
/// the range-tombstone escape hatch must run the handle-based scrub — which
/// here finds the corrupt data block and refuses the keep.
#[cfg(feature = "page_ecc")]
#[test]
fn repair_with_salvage_rejects_a_corrupt_unrecognized_ecc_tombstone_sst_with_parity_errors()
-> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::table::block::EccParams;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        // An ECC table: its meta blocks carry SELF-DESCRIBING parity, which
        // the forge below leaves stale (it re-stamps the payload checksum
        // without recomputing the trailer) — producing exactly the
        // EccParityMismatch-only error set on a walk that skipped the data.
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_ecc(Some(EccParams::try_new(4, 2)?));
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    forge_unrecognized_ecc_descriptor(&sst)?;
    // Corrupt the sole data block: the out-of-band walk cannot see it (the
    // data section is skipped under the unrecognized descriptor), so only
    // the handle-based fallback can catch it.
    let block_off = usize::try_from(sole_data_block_offset(&recover_table(sst.clone(), &fs)?))
        .unwrap_or(usize::MAX);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(block_off + 40) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "parity-only meta errors must not mask the unscanned data sections: {report:?}",
    );
    assert_eq!(
        report.unreadable_files.len(),
        1,
        "the corrupt table is reported unreadable: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// The fallback verification behind the unscanned escape hatch must also
/// cover the LAZY side blocks a data scrub never touches: the full bloom
/// filter only loads on the first point read, so a table with clean data
/// blocks but a corrupt filter would otherwise be kept and fail point reads
/// once the rebuilt manifest goes live.
#[test]
fn repair_with_salvage_rejects_an_unrecognized_ecc_tombstone_sst_with_a_corrupt_filter()
-> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..200u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Forge the unrecognized descriptor and corrupt the FILTER section: the
    // out-of-band walk skips it (unrecognized descriptor), the data scrub
    // never loads it (the full bloom filter is lazy), so only a point read
    // can surface the damage.
    forge_unrecognized_ecc_descriptor(&sst)?;
    let (filter_pos, filter_len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"filter") else {
            panic!("the SST must carry a filter section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(filter_pos + filter_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a table whose lazy filter is corrupt must not be kept over the \
         range-tombstone escape hatch: {report:?}",
    );
    assert_eq!(
        report.unreadable_files.len(),
        1,
        "the unverifiable table is reported unreadable: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// Patches `descriptor#page_ecc` to a non-canonical (unrecognized) value in
/// both meta blocks of the SST at `path`, re-stamping each block's checksum
/// so the frames stay checksum-clean.
/// Overwrites the TAIL meta copy's `table_id` value with `forged_id` (when
/// `Some`) and/or its `descriptor#page_ecc` value with `forge_descriptor`
/// (when `Some` — an unrecognized OR forged-recognized 4-byte descriptor),
/// restamping that block's checksum and leaving the mirrored `meta_mid`
/// copy intact — the "only the tail rotted" scenario a normal recovery
/// survives via its expected-id cross-check + MID fallback.
fn forge_tail_meta_table_id(
    path: &std::path::Path,
    forged_id: Option<u64>,
    forge_descriptor: Option<[u8; 4]>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"meta") else {
            panic!("the SST must carry a meta section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).unwrap_or(usize::MAX);
    let Some(block) = bytes.get(block_off..) else {
        panic!("meta block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("meta payload within the file");
        };
        if let Some(forged_id) = forged_id {
            let needle = b"table_id";
            let Some(key_pos) = payload
                .windows(needle.len())
                .position(|w| w == needle.as_slice())
            else {
                panic!("table_id key present verbatim (restart interval 1)");
            };
            // Entry layout after the key bytes: value length (LEB128, one
            // byte for 8), then the 8-byte little-endian id.
            let val_at = key_pos + needle.len();
            assert_eq!(
                payload.get(val_at).copied(),
                Some(8),
                "table_id value length prefix",
            );
            let Some(value) = payload.get_mut(val_at + 1..val_at + 9) else {
                panic!("table_id value within the payload");
            };
            value.copy_from_slice(&forged_id.to_le_bytes());
        }

        if let Some(descriptor) = forge_descriptor {
            let needle = b"descriptor#page_ecc";
            let Some(key_pos) = payload
                .windows(needle.len())
                .position(|w| w == needle.as_slice())
            else {
                panic!("descriptor key present verbatim (restart interval 1)");
            };
            let val_at = key_pos + needle.len();
            assert_eq!(
                payload.get(val_at).copied(),
                Some(4),
                "descriptor value length prefix",
            );
            let Some(value) = payload.get_mut(val_at + 1..val_at + 5) else {
                panic!("descriptor value within the payload");
            };
            value.copy_from_slice(&descriptor);
        }
    }
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("meta payload within the file");
    };
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("meta header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    // A parity-bearing meta frame (self-describing blocks always use the
    // fixed RS(4,2) layout) must have its trailer recomputed over the
    // forged payload, or the walk would flag the forge ITSELF as parity
    // rot and mask what a test actually exercises.
    #[cfg(feature = "page_ecc")]
    {
        let payload_end = payload_range.end;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("meta section length fits usize");
        };
        let frame_end = block_off + section_len;
        if frame_end > payload_end {
            let Some(payload) = bytes.get(payload_range) else {
                panic!("meta payload within the file");
            };
            let parity = crate::ecc::encode_parity(payload, 4, 2)?;
            assert_eq!(
                frame_end - payload_end,
                parity.len(),
                "the meta frame's trailer length matches the fixed RS(4,2) layout",
            );
            let Some(dst) = bytes.get_mut(payload_end..frame_end) else {
                panic!("meta parity trailer within the file");
            };
            dst.copy_from_slice(&parity);
        }
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = section_len;

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// A repair salvage knows the durable table id from the SST's file name; the
/// salvage-mode open must cross-check it so a checksum-clean TAIL meta whose
/// `table_id` field was forged falls back to the intact MID mirror (exactly
/// like normal recovery) instead of stamping the recovered copy with the
/// forged id — which would fail the post-salvage reopen under the file-name
/// id and quarantine a recoverable table.
#[test]
fn repair_with_salvage_preserves_the_file_name_id_over_a_forged_tail_meta_id() -> crate::Result<()>
{
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, MAX_SEQNO, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // Enough data for SEVERAL blocks: one gets corrupted (triggering salvage),
    // the rest stay recoverable.
    {
        let mut w = Writer::new(sst.clone(), 7, 0, Arc::clone(&fs))?;
        for i in 0..600u32 {
            w.write(InternalValue::from_components(
                format!("key{i:05}").into_bytes(),
                format!("{i:08}").repeat(8).into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt the FIRST data block so verification routes the table through
    // salvage.
    let offset = {
        let checksum = crate::Checksum::from_raw(compute_table_checksum(&*fs, &sst)?);
        let table = crate::table::Table::recover(
            sst.clone(),
            checksum,
            0,
            0,
            7,
            Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
            None,
            Arc::clone(&fs),
            false,
            false,
            None,
            #[cfg(zstd_any)]
            None,
            crate::comparator::default_comparator(),
            #[cfg(feature = "metrics")]
            Arc::new(crate::Metrics::default()),
        )?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        assert!(offsets.len() >= 2, "need several blocks, got {offsets:?}");
        let Some(&first) = offsets.first() else {
            panic!("a first data block exists");
        };
        first
    };
    let flip = usize::try_from(offset).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    // Forge ONLY the tail meta's table_id (7 → 99); the MID mirror keeps 7.
    forge_tail_meta_table_id(&sst, Some(99), None)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the readable blocks are salvaged under the file-name id: {:?}",
        report.unreadable_files,
    );
    assert_eq!(
        report.unreadable, 0,
        "no quarantine for a recoverable table: {:?}",
        report.unreadable_files,
    );

    // The recovered copy reopens under the durable file-name id and serves
    // the surviving keys.
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        unreachable!("standard tree");
    };
    let got = tree.get(b"key00599", MAX_SEQNO)?;
    assert!(
        got.is_some(),
        "a key outside the corrupt block survives the salvage",
    );
    Ok(())
}

fn forge_unrecognized_ecc_descriptor(path: &std::path::Path) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let sections: Vec<(u64, u64)> = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        [b"meta".as_slice(), b"meta_mid".as_slice()]
            .iter()
            .map(|name| {
                let Some(entry) = reader.toc().iter().find(|e| e.name() == *name) else {
                    panic!(
                        "the SST must carry a {} section",
                        String::from_utf8_lossy(name)
                    );
                };
                (entry.pos(), entry.len())
            })
            .collect()
    };
    for (pos, _len) in sections {
        let block_off = usize::try_from(pos).unwrap_or(usize::MAX);
        let Some(block) = bytes.get(block_off..) else {
            panic!("meta block within the file");
        };
        let mut cursor = block;
        let header = Header::decode_from(&mut cursor)?;
        let header_len = Header::header_len(header.block_type);
        let payload_range =
            block_off + header_len..block_off + header_len + header.data_length as usize;
        {
            let Some(payload) = bytes.get_mut(payload_range.clone()) else {
                panic!("meta payload within the file");
            };
            let needle = b"descriptor#page_ecc";
            let Some(key_pos) = payload
                .windows(needle.len())
                .position(|w| w == needle.as_slice())
            else {
                panic!("descriptor key present verbatim (restart interval 1)");
            };
            // Entry layout after the key bytes: value length (LEB128, one
            // byte for 4), then the 4-byte descriptor value.
            let val_at = key_pos + needle.len();
            assert_eq!(
                payload.get(val_at).copied(),
                Some(4),
                "descriptor value length prefix",
            );
            let Some(value) = payload.get_mut(val_at + 1..val_at + 5) else {
                panic!("descriptor value within the payload");
            };
            assert_ne!(value, [0u8, 8, 2, 1], "descriptor not already forged");
            value.copy_from_slice(&[0u8, 8, 2, 1]);
        }
        let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
            bytes.get(payload_range).unwrap_or(&[]),
        ));
        let new_header = Header {
            checksum: new_checksum,
            ..header
        };
        let mut hdr_bytes = Vec::with_capacity(header_len);
        new_header.encode_into(&mut hdr_bytes)?;
        let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
            panic!("meta header within the file");
        };
        hdr_dst.copy_from_slice(&hdr_bytes);
    }
    std::fs::write(path, &bytes)?;
    Ok(())
}

/// `repair_with_salvage` QUARANTINES an SST whose delete-bitmap section is
/// corrupt rather than recovering it: whole-file recovery refuses it (a corrupt
/// bitmap would resurrect deleted rows) and automated salvage fails closed for
/// the same reason — the "all rows live" degradation is an explicit
/// `SalvageOptions::allow_delete_resurrection` opt-in that automated repair
/// never takes. The original stays in quarantine for a manual opt-in salvage.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_quarantines_a_corrupt_delete_bitmap_sst() -> crate::Result<()> {
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A columnar SST (table id 0) carrying a delete-bitmap.
    let n = 200u32;
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_columnar(true)
            .use_zone_map(true)
            .delete_strategy(DeleteStrategy::MergeOnRead);
        for i in 0..n {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        for pos in [5u32, 50, 150] {
            w.delete_bitmap_mut().insert(pos);
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt the middle of the delete_bitmap section so normal recovery refuses
    // the SST (the data blocks stay intact).
    let (pos, len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("the SST must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(pos + len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "automated repair refuses to resurrect deleted rows: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("salvage failed") && reason.contains("resurrect"),
        "the reason names the refused delete resurrection, got: {reason}",
    );
    Ok(())
}

/// `repair_with_salvage` must QUARANTINE (not salvage) an SST whose TOC HIDES a
/// deletion section: an omitted `range_tombstones` entry makes the parsed table
/// report NO tombstones, so the positional salvage walk would re-emit the keys
/// the tombstone covered as LIVE — resurrecting data the deletion suppressed.
/// Unlike a corrupt-but-present deletion section (which the salvage guard
/// catches on the parsed state), a hidden section is invisible to that guard;
/// the catalogue tiling gap is the only trace, so the repair verdict must
/// refuse salvage before it reopens the forged catalogue.
#[test]
fn repair_with_salvage_quarantines_a_toc_hidden_range_tombstone_sst() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        // The range tombstone gives the SST the optional `range_tombstones`
        // section whose hiding resurrects the keys it covers.
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Drop the `range_tombstones` TOC entry: the parsed table now reports no
    // tombstones (the covered keys look live), and the only out-of-band trace
    // is the gap the omission leaves in the section tiling.
    crate::test_forge::forge_section_omitted(&sst, b"range_tombstones")?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;

    assert_eq!(
        report.salvaged, 0,
        "salvage must not re-emit a TOC-hidden tombstone's covered keys as \
         live: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    assert_eq!(
        report.unreadable_files.len(),
        1,
        "the hidden-deletion table is reported unreadable: {:?}",
        report.unreadable_files,
    );
    // Pin the specific gate: a whole-file recovery failure would quarantine
    // with the same counts, so require the refusal to name the TOC-concealment
    // check rather than accepting any quarantine path.
    assert!(
        report
            .unreadable_files
            .iter()
            .any(|(_, reason)| reason.contains("may hide deletion metadata")),
        "the refusal must come from the TOC concealment gate: {:?}",
        report.unreadable_files,
    );
    // The original SST is preserved in quarantine for manual recovery, not
    // left in `tables/` (where the next open's orphan cleanup would delete it).
    let quarantine = dir.path().join("repair-quarantine").join("0");
    assert!(
        quarantine.exists(),
        "the original SST must be quarantined, got {:?}",
        report.unreadable_files,
    );
    assert!(
        !tables.join("0").exists(),
        "the corrupt original must not stay in tables/",
    );
    Ok(())
}

/// `repair_with_salvage` must QUARANTINE (not salvage) a table whose
/// `range_tombstones` section is RENAMED to another recognized name (here
/// `filter_tli`) with its block role re-stamped to match. The catalogue stays
/// uniquely named and perfectly tiled, so the deletion-hiding TOC check clears
/// it, but the relabeled section is graded corrupt (its bytes are not a filter
/// index) and salvage would DISCARD it while re-emitting the covered keys as
/// live, resurrecting the deletion. A corrupt REBUILDABLE side section must
/// fail closed: it may be a relabeled deletion salvage cannot see.
#[test]
fn repair_with_salvage_quarantines_a_range_tombstone_renamed_to_a_rebuildable_section()
-> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::table::block::BlockType;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Rename `range_tombstones` -> `filter_tli` and re-stamp its block role to
    // Index: the parsed table now reports no tombstones, and the catalogue is
    // uniquely named and tiled.
    crate::test_forge::forge_duplicate_section_name(
        &sst,
        b"range_tombstones",
        b"filter_tli",
        BlockType::Index,
    )?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;

    assert_eq!(
        report.salvaged, 0,
        "a relabeled range_tombstones must not be salvaged into a live copy: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    // Pin the specific gate: a generic whole-file recovery failure would
    // quarantine with the same counts. This table carried a range tombstone, so
    // the persisted-count cross-check refuses it (ahead of the degraded-section
    // flag, which a delete-free relabel exercises separately).
    assert!(
        report
            .unreadable_files
            .iter()
            .any(|(_, reason)| reason.contains("range tombstones")),
        "the refusal must come from the range-tombstone gate: {:?}",
        report.unreadable_files,
    );
    assert!(
        dir.path().join("repair-quarantine").join("0").exists(),
        "the relabeled table must be quarantined for manual recovery: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// A tail meta whose ECC descriptor is forged to a DIFFERENT recognized
/// scheme (here: `Off`) while its table id stays valid must not dictate the
/// walk's trailer sizing: the probe must arbitrate against the intact MID
/// mirror, and when two decodable copies disagree, fail safe (skip the
/// ECC-dependent sections with a warning, plus the single mirror-divergence
/// finding) instead of mis-walking parity bytes as block headers and
/// condemning a healthy SST.
#[cfg(feature = "page_ecc")]
#[test]
fn verify_probe_distrusts_disagreeing_recognized_ecc_descriptors() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 7, 0, Arc::clone(&fs))?
            .use_ecc(Some(crate::table::block::EccParams::RS_4_2));
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Forge ONLY the tail descriptor to canonical `Off` (a RECOGNIZED
    // state); the id stays valid, so the expected-id cross-check passes,
    // and the MID mirror keeps the true RS descriptor.
    forge_tail_meta_table_id(&sst, None, Some([0u8, 0, 0, 0]))?;

    let report = crate::verify::verify_sst_file_with_fs(&*fs, &sst);
    // The forged tail IS a real finding: the full mirror comparison reports
    // the divergence. What must NOT happen is the walk mis-sizing every
    // parity trailer as a block header and condemning the data blocks — so
    // the only error is the single mirror-divergence finding, never a
    // HeaderCorrupted storm.
    assert!(
        report
            .errors
            .iter()
            .all(|e| matches!(e, crate::verify::BlockVerifyError::TocCorrupted { .. })),
        "a forged recognized descriptor must not condemn the data blocks — \
         the walk mis-sizes every parity trailer as a block header: {report:?}",
    );
    assert_eq!(
        report.errors.len(),
        1,
        "exactly the mirror-divergence finding: {report:?}",
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|w| matches!(w, crate::verify::BlockVerifyWarning::UnrecognizedEcc { .. })),
        "disagreeing decodable descriptors must surface as an \
         indeterminate-ECC warning: {report:?}",
    );
    Ok(())
}

/// The per-KV gate must run BEFORE the parity-only degradation arm: on a
/// footer-bearing Page-ECC SST, a stale footer behind a re-stamped block
/// checksum also leaves the parity trailer mismatched, so the walk reports
/// ONLY `EccParityMismatch` and the verdict graded the table
/// `DegradedButReadable` — with range tombstones (which salvage refuses to
/// re-emit) the keep-decision then rebuilt the manifest around an entry
/// whose per-KV digest is known stale, instead of quarantining the table
/// as corrupt.
#[cfg(feature = "page_ecc")]
#[test]
fn repair_grades_a_stale_kv_footer_corrupt_over_parity_only_degradation() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A footer-bearing Page-ECC SST WITH a range tombstone, so salvage
    // refuses it and the keep-decision path is the one under test.
    {
        use crate::runtime_config::{ChecksumAlgorithm, KvChecksumPolicy};

        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_ecc(Some(crate::table::block::EccParams::RS_4_2))
            .use_kv_checksums(KvChecksumPolicy::AllLevels, ChecksumAlgorithm::Xxh3_64);
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        w.write_range_tombstone(RangeTombstone::new(
            UserKey::from(b"k00002".as_slice()),
            UserKey::from(b"k00005".as_slice()),
            2,
        ));
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Stale footer + re-stamped block checksum: the parity trailer (not
    // re-stamped) now mismatches too, so the walk reports ONLY
    // EccParityMismatch while the block checksum reads clean.
    crate::test_forge::forge_stale_kv_footer(&sst)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;

    // The table is KNOWN corrupt (stale per-KV digest), and salvage cannot
    // re-emit its range tombstones: it must be quarantined, never kept as a
    // merely parity-degraded table.
    assert_eq!(
        report.recovered, 0,
        "a stale-footer table must not be kept as parity-only degradation: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the corrupt table lands in quarantine: {report:?}",
    );
    Ok(())
}

/// The repair verdict must not declare a table clean on block checksums
/// alone: a STALE per-KV footer behind a re-stamped block checksum passes
/// the out-of-band walk, so repair would record a fresh whole-file digest
/// over a table `verify_kv_checksums` rejects — while the salvage row path
/// (which repair skipped) validates footers and would have dropped the
/// forged block.
#[test]
fn repair_routes_a_stale_kv_footer_through_salvage() -> crate::Result<()> {
    use crate::runtime_config::KvChecksumPolicy;
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;

    // Flush one footer-bearing, uncompressed SST.
    let sst_path = {
        let crate::AnyTree::Standard(tree) = Config::new(
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
        tree.update_runtime_config(|c| c.kv_checksums = KvChecksumPolicy::AllLevels)?;
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

    // Forge a stale footer behind a re-stamped block checksum so the
    // block-level walk reads clean while per-KV verification rejects it.
    crate::test_forge::forge_stale_kv_footer(&sst_path)?;

    // Repair with salvage: the verdict must route the table through
    // salvage (which drops the forged block) instead of recording a fresh
    // digest over content the per-KV scrub rejects.
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the stale-footer table must be routed through salvage: {report:?}",
    );

    // The salvaged copy passes per-KV verification (the forged block was
    // dropped, not laundered into the copy).
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        unreachable!("standard tree configured");
    };
    crate::verify::verify_kv_checksums(&tree)?;
    Ok(())
}

/// A FOOTER-LESS SST whose data block declares more entries than it decodes
/// (a re-stamped trailer item count) must be routed through salvage, not
/// graded Clean. `verify_kv_checksums` is a no-op without footers and the
/// out-of-band walk verifies only the outer frame, so only a full-decode
/// completeness check catches the truncated tail before repair rebuilds the
/// manifest around a block whose keys a later scan silently omits.
#[test]
fn repair_routes_an_under_decoding_footerless_block_through_salvage() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;

    // Flush one uncompressed, FOOTER-LESS SST (default kv_checksums = Off).
    let sst_path = {
        let crate::AnyTree::Standard(tree) = Config::new(
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

    // Inflate the first data block's trailer item count behind a re-stamped
    // block checksum: iteration yields fewer entries than declared.
    crate::test_forge::forge_inflated_item_count(&sst_path)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the under-decoding footer-less table must be routed through salvage: {report:?}",
    );
    Ok(())
}

/// `is_corruption` grades a block-verify result for the salvage gate. Only the
/// transient allowlist (`Interrupted`, `WouldBlock`) aborts the repair for a
/// retry; a PERSISTENT I/O failure — a genuine bad sector, or a structural
/// corruption that surfaces as `Io(Other)` on some platforms (e.g. Windows
/// negative-seek) — is not resolved by a retry, so it must be graded as
/// corruption (`Ok(true)`) and routed to salvage rather than aborting the whole
/// repair and permanently stranding every other healthy table.
#[test]
fn is_corruption_routes_a_persistent_io_to_salvage() {
    let persistent = crate::Error::Io(crate::io::Error::other("bad sector"));
    assert!(
        matches!(super::is_corruption(Err(persistent)), Ok(true)),
        "a persistent I/O failure must grade as corruption, not abort the repair",
    );
}

/// The mirror of [`is_corruption_routes_a_persistent_io_to_salvage`]: a genuine
/// transient failure still aborts the repair so the caller can retry, rather
/// than dropping a healthy block into a partial salvaged replacement.
#[test]
fn is_corruption_aborts_the_repair_on_a_transient_io() {
    let transient = crate::Error::Io(crate::io::Error::from_kind(
        crate::io::ErrorKind::Interrupted,
    ));
    assert!(
        super::is_corruption(Err(transient)).is_err(),
        "a transient I/O failure must propagate so the repair can retry",
    );
}
