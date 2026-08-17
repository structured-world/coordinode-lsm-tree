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

/// A tight-space-punched, RESTRICTED SST whose WHOLE-FILE recovery fails
/// persistently (a bad sector on the preliminary hash) is block-salvaged AND
/// re-restricted to its sidecar bound. The recovery-failure salvage arm produced
/// no `Table` to read the bound from, so it reads the `.restrict-bound` sidecar
/// directly before quarantining and reopens the salvaged replacement restricted:
/// no superseded sub-bound row resurrects under the default fail-closed policy.
/// Without the re-restriction the salvage walk re-emits the straddling block's
/// sub-bound rows unrestricted.
#[test]
fn repair_restricts_a_punched_sst_whose_whole_file_recovery_faults() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;

    // A multi-block SST punched at k00050, its exact bound recorded in the sidecar.
    let sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;
    let bound = b"k00050".to_vec();
    crate::restrict_bound::write(&*fs, &sst, None, 0, &bound, crate::fs::SyncMode::Normal)?;

    // Fault the FIRST streaming read of the original (the whole-file hash) so
    // whole-file recovery fails structurally and repair falls to block-salvage.
    // `.once()` leaves the sidecar read and the salvage reads unfaulted.
    let fault = FaultFs::new(memfs.as_ref().clone());
    let injector = fault.injector();
    injector.arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    injector.clear();

    assert_eq!(
        report.recovered, 1,
        "the salvaged table joins the manifest: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // No sub-bound key resurrects, despite the whole-file recovery failure.
    for i in 0..50u32 {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_none(),
            "sub-bound key {} must not resurrect after recovery-failure salvage",
            String::from_utf8_lossy(&key),
        );
    }
    // The live suffix survives.
    assert!(
        tree.get(b"k00200", crate::MAX_SEQNO)?.is_some(),
        "the live suffix must survive salvage",
    );
    Ok(())
}

/// A BULK-INGESTED SST whose whole-file recovery fails must NOT be recovered
/// through block-salvage: `try_salvage_table` reopens the salvaged copy with
/// `global_seqno` 0, and the copy still relies on the manifest-only offset (its
/// entries stay at local seqno 0), so registering it would silently mis-order
/// them. The salvage guard drops the copy and records the table unreadable.
#[test]
fn repair_with_salvage_quarantines_a_bulk_ingested_sst_that_fails_recovery() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");

    // A bulk-ingested SST (flag set, entries at local seqno 0).
    {
        let build_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&build_fs))?
            .use_bulk_ingested(Some(true))
            .use_data_block_size(128);
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                0,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Fail whole-file recovery (persistent hash fault) so the table routes to
    // block-salvage; salvage then reopens the copy, which carries the flag.
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
        report.recovered, 0,
        "a bulk-ingested SST whose offset is lost must not be salvaged-and-kept: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the bulk-ingested SST is reported unreadable: {:?}",
        report.unreadable_files,
    );

    // The rejected salvage replacement must be QUARANTINED, not removed-and-forgotten:
    // a discarded removal error would leave it as a numeric orphan in `tables/` that
    // blocks the next open. `repair-quarantine/` therefore holds BOTH the corrupt
    // original (set aside by the caller) AND the rejected replacement.
    let quarantine = dir.path().join("repair-quarantine");
    let quarantined: usize = std::fs::read_dir(&quarantine)
        .into_iter()
        .flatten()
        .flatten()
        .count();
    assert_eq!(
        quarantined, 2,
        "both the corrupt original and the rejected salvage replacement must be \
         quarantined (found {quarantined})",
    );
    // No numeric SST may linger in `tables/` to orphan the next open.
    let orphans: usize = std::fs::read_dir(dir.path().join("tables"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .count();
    assert_eq!(orphans, 0, "no rejected replacement may linger in tables/");
    Ok(())
}

/// A LEGACY SST (no `descriptor#bulk_ingested` key at all — written before the
/// descriptor existed) whose entries sit at local seqno 0 has UNKNOWN provenance:
/// it may have been bulk-ingested with a manifest-only `global_seqno`. When
/// whole-file recovery fails and it routes to block-salvage, the salvaged copy
/// must PRESERVE that unknown (`None`) provenance, not stamp "not ingested" —
/// otherwise the salvage guard's seqno heuristic never fires and the table is
/// kept with `global_seqno` 0, silently mis-ordering it. The mirror writer omits
/// the key for a `None` source, so the reopened copy re-parses as `None` and the
/// guard quarantines it.
#[test]
fn repair_with_salvage_quarantines_a_legacy_seqno0_sst_that_fails_recovery() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");

    // A LEGACY SST: provenance UNKNOWN (no flag key), entries at local seqno 0.
    {
        let build_fs: Arc<dyn Fs> = Arc::new(StdFs);
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&build_fs))?
            .use_bulk_ingested(None)
            .use_data_block_size(128);
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                0,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Fail whole-file recovery (persistent hash fault) so the table routes to
    // block-salvage; salvage reopens the copy, which must re-parse as `None`.
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
        report.recovered, 0,
        "a legacy seqno-0 SST of unknown provenance must not be salvaged-and-kept: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the legacy seqno-0 SST is reported unreadable: {:?}",
        report.unreadable_files,
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

/// A table flagged `descriptor#bulk_ingested` must be QUARANTINED, not
/// registered with `global_seqno` 0. A bulk-ingested SST stores every entry at
/// local seqno 0 and relies on a manifest-only offset for its effective MVCC
/// ordering; the rebuilt manifest cannot recover that offset from the SST, so
/// keeping the table with offset 0 would make its entries appear older than they
/// are (MVCC corruption). Repair fails closed instead. The flag is precise — a
/// normal flush at seqno 0 (unflagged) is recovered as usual (see
/// `repair_clears_torn_edit_log_tail_and_reopens_under_default`).
#[test]
fn repair_quarantines_a_table_with_an_unrecoverable_ingest_offset() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A bulk-ingested table: entries at local seqno 0, flagged bulk-ingested (as
    // the ingest path writes them), so its effective ordering lives in a
    // manifest-only global_seqno the rebuilt manifest cannot recover.
    {
        let mut w = Writer::new(sst, 0, 0, Arc::clone(&fs))?.use_bulk_ingested(Some(true));
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                0,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    assert_eq!(
        report.recovered, 0,
        "a table with an unrecoverable ingest offset must not join the manifest: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the ambiguous-offset table is reported unreadable: {:?}",
        report.unreadable_files,
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("sequence offset"),
        "the reason names the unrecoverable ingest offset, got: {reason}",
    );
    Ok(())
}

/// `has_unrecoverable_ingest_offset` must fail closed on an authoritatively
/// bulk-ingested table AND on a LEGACY table (absent flag) whose entries carry
/// the ingest signature (all at local seqno 0), while keeping a newer
/// non-ingested table — even one whose entries happen to sit at seqno 0.
#[test]
fn has_unrecoverable_ingest_offset_classifies_provenance() {
    use super::has_unrecoverable_ingest_offset;

    // Authoritative flag wins outright.
    assert!(has_unrecoverable_ingest_offset(Some(true), 8, 0));
    assert!(has_unrecoverable_ingest_offset(Some(true), 8, 5));
    // A newer non-ingested table is safe, even at all-seqno-0 (offset genuinely 0).
    assert!(!has_unrecoverable_ingest_offset(Some(false), 8, 0));
    assert!(!has_unrecoverable_ingest_offset(Some(false), 8, 5));
    // Legacy (absent flag): the ingest signature (entries present, max local
    // seqno 0) is treated as ambiguous → fail closed.
    assert!(has_unrecoverable_ingest_offset(None, 8, 0));
    // Legacy with a non-zero max seqno cannot be all-local-0 → safe.
    assert!(!has_unrecoverable_ingest_offset(None, 8, 5));
    // Legacy empty table: nothing to mis-order.
    assert!(!has_unrecoverable_ingest_offset(None, 0, 0));
}

/// Opens an SST as a `Table` under a given filesystem, stamping the open with
/// the file's CURRENT whole-file digest (matching what repair computes). Used to
/// inspect block layout and to re-read a punched file.
fn recover_sst(
    path: std::path::PathBuf,
    fs: &std::sync::Arc<dyn crate::fs::Fs>,
) -> crate::Result<crate::Table> {
    let checksum = crate::Checksum::from_raw(compute_table_checksum(&**fs, &path)?);
    crate::Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        std::sync::Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(std::sync::Arc::new(
            crate::descriptor_table::DescriptorTable::new(8),
        )),
        std::sync::Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        std::sync::Arc::new(crate::Metrics::default()),
    )
}

/// A tight-space-PUNCHED SST records its restriction bound only in the manifest.
/// When manifest repair rebuilds without that bound, the punched table would open
/// UNRESTRICTED and later reads would traverse its zero-reading (punched) blocks
/// and fail. Compaction records the exact bound in a `.restrict-bound` sidecar
/// beside the SST (without touching the SST). Repair reads the sidecar and — after
/// confirming the prefix is really punched — restricts to the EXACT bound, so a
/// MID-BLOCK boundary recovers with zero loss of the block's live suffix (#60) and
/// zero resurrection of its consumed prefix. Probes every key: served IFF key >=
/// exact bound. Uses `MemFs` for a byte-precise punch.
#[test]
fn repair_restricts_a_tight_space_punched_table() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    // An SST with several small data blocks so a prefix can be punched.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..256u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // A restriction bound chosen to fall MID-BLOCK: `k00130` is a real key, and
    // its straddling block also holds keys below it (the consumed prefix repair
    // must hide) and at/above it (the live suffix repair must keep).
    let bound = b"k00130".to_vec();
    {
        let table = recover_sst(sst.clone(), &fs)?;

        // Publish the exact bound to the sidecar (what tight-space compaction does
        // before punching), then punch the consumed prefix. `punch_offset_for`
        // returns the offset of the block that STRADDLES the bound, so `[0, offset)`
        // is the whole blocks strictly below it; the straddling block (holding both
        // the sub-bound consumed keys and the live suffix) survives. The
        // served-iff-key>=bound probe below proves the mid-block split is exact.
        crate::restrict_bound::write(&*fs, &sst, None, 0, &bound, crate::fs::SyncMode::Normal)?;
        let punch_offset = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, punch_offset)?;
    }

    // Repair rebuilds the manifest, recovering the punched table RESTRICTED to the
    // EXACT bound read from the sidecar.
    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair()?;
    assert_eq!(
        report.recovered, 1,
        "the punched table is recovered restricted: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    // Reopen and probe EVERY key: served IFF key >= exact bound. The lower half
    // (no key below the bound served) is the no-resurrection guard; the upper half
    // (every key at/above the bound served, INCLUDING the straddling block's live
    // suffix `[k00130, block_end)`) is the no-loss guard that #60 demands.
    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    for i in 0..256u32 {
        let key = format!("k{i:05}").into_bytes();
        let served = tree.get(&key, crate::MAX_SEQNO)?.is_some();
        assert_eq!(
            served,
            key.as_slice() >= bound.as_slice(),
            "key {key:?} served={served}; expected served == (key >= {bound:?}) \
             (a served key below the bound is resurrected; an unserved key at/above \
             it is lost)",
        );
    }
    Ok(())
}

/// With salvage ENABLED, an otherwise-healthy tight-space RESTRICTED SST (a valid
/// `.restrict-bound` sidecar, fully punch-backed) must be KEPT restricted, not
/// salvaged away. The salvage-gate block walk must start at the view's live data
/// start (`punch_offset`), not byte 0: walking the hole-punched `[0, punch)` prefix
/// reads zeroed block headers, reports them as corruption, and the restricted-table
/// safeguard then quarantines the healthy SST, dropping it from the manifest
/// precisely because salvage was enabled (#79).
#[test]
fn repair_with_salvage_keeps_a_healthy_restricted_punched_table() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    write_multiblock_sst(&sst, &fs)?;

    // A mid-block bound, published to the sidecar, with the whole prefix below it
    // punched: a legitimately restricted, otherwise-healthy view.
    let bound = b"k00130".to_vec();
    {
        let table = recover_sst(sst.clone(), &fs)?;
        crate::restrict_bound::write(&*fs, &sst, None, 0, &bound, crate::fs::SyncMode::Normal)?;
        let punch = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, punch)?;
    }

    // Salvage ENABLED is the trigger: the salvage gate's block walk runs only here.
    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "a healthy restricted SST must be kept restricted even with salvage on: {report:?}",
    );
    assert_eq!(
        report.unreadable, 0,
        "the healthy restricted SST must NOT be quarantined: {:?}",
        report.unreadable_files,
    );

    // Probe every key: served IFF key >= bound, so the restriction survived intact.
    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    for i in 0..256u32 {
        let key = format!("k{i:05}").into_bytes();
        let served = tree.get(&key, crate::MAX_SEQNO)?.is_some();
        assert_eq!(
            served,
            key.as_slice() >= bound.as_slice(),
            "key {key:?} served={served}; expected served == (key >= {bound:?})",
        );
    }
    Ok(())
}

/// Builds a multi-data-block SST under `fs` at `path` (keys `k00000..k00255`).
fn write_multiblock_sst(
    path: &std::path::Path,
    fs: &std::sync::Arc<dyn crate::fs::Fs>,
) -> crate::Result<()> {
    use crate::{InternalValue, ValueType};
    let mut w = crate::table::Writer::new(path.to_path_buf(), 0, 0, std::sync::Arc::clone(fs))?
        .use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    assert!(w.finish()?.is_some(), "the SST is non-empty");
    Ok(())
}

/// Builds a multi-block SST at `tables/0` under `memfs`, then punches its consumed
/// prefix `[0, punch(k00050))`, the on-disk state of a tight-space-PUNCHED SST.
/// The caller then publishes (or withholds / corrupts / mismatches) a
/// `.restrict-bound` sidecar to drive repair through each no-trustworthy-bound
/// path. Returns the SST path.
fn build_punched_prefix_sst(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    fs: &std::sync::Arc<dyn crate::fs::Fs>,
    tables: &std::path::Path,
) -> crate::Result<std::path::PathBuf> {
    use crate::fs::Fs;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, fs)?;
    let committed = recover_sst(sst.clone(), fs)?.punch_offset_for(b"k00050")?;
    memfs.punch_hole(&sst, 0, committed)?;
    Ok(sst)
}

/// Builds a multi-block SST at `tables/0`, then PARTIALLY punches its consumed
/// prefix `[0, punch(k00050))`: every prefix data block EXCEPT THE FIRST is
/// zeroed, modeling a punch-on-drop reclaim whose first `punch_hole` failed (the
/// reclaim logs and continues per block). The first block stays intact while
/// later prefix blocks read as zeros — the state a first-block-only punch probe
/// misreads as "unpunched". No sidecar is published. Returns the SST path.
fn build_partially_punched_prefix_sst(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    fs: &std::sync::Arc<dyn crate::fs::Fs>,
    tables: &std::path::Path,
) -> crate::Result<std::path::PathBuf> {
    use crate::fs::Fs;
    use crate::table::block_index::BlockIndex;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, fs)?;
    let table = recover_sst(sst.clone(), fs)?;
    let committed = table.punch_offset_for(b"k00050")?;
    let mut punched = 0u32;
    for handle in table.block_index.iter() {
        let handle = handle?;
        let off = handle.offset().0;
        if off > 0 && off < committed {
            memfs.punch_hole(&sst, off, u64::from(handle.size()))?;
            punched += 1;
        }
    }
    assert!(
        punched >= 2,
        "precondition: the consumed prefix spans several blocks past the first",
    );
    Ok(sst)
}

/// A PARTIALLY punched SST (intact first block, zeroed later prefix blocks)
/// with no trustworthy sidecar must be SET ASIDE under default repair
/// (resurrection off): the interleaved intact block is positive evidence that
/// `punch_hole` failed mid-reclaim, and then ANY readable block above the last
/// hole may equally be an intact-but-consumed block whose punch also failed —
/// no geometry bound can separate consumed from live, so restricting to one
/// would resurrect superseded rows. Only a CLEAN zeroed prefix (no intact
/// block below a zeroed one) supports the classical geometry bound.
#[test]
fn repair_sets_aside_a_partially_punched_sst_without_a_trustworthy_bound() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "an irregularly punched SST with no exact bound must be set aside, not \
         restricted to a guessed bound that resurrects consumed rows: {report:?}",
    );
    assert_eq!(report.unreadable, 1, "{:?}", report.unreadable_files);
    assert!(
        report
            .unreadable_files
            .first()
            .is_some_and(|(_, reason)| reason.contains("punch failures")),
        "the reason names the failed punches that made the bound unknowable: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// The RESURRECTION counterpart with salvage OFF: a partially punched SST
/// (intact first block, zeroed later prefix blocks, no sidecar) must be
/// restricted PAST the last zeroed block. Unrestricted, a read in the zeroed
/// region would error after a supposedly successful repair; a bound anchored at
/// the intact FIRST block would leave the zeroed blocks inside the served view
/// with the same effect.
#[test]
fn resurrection_restricts_a_partially_punched_sst_past_the_zeroed_blocks() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(false, true)?;
    assert_eq!(report.recovered, 1, "{report:?}");
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // A key in a ZEROED prefix block must miss CLEANLY: unrestricted (or
    // restricted only to the intact first block's key) this get would route to
    // a zeroed block and error, failing the test through the `?`.
    assert!(
        tree.get(b"k00020", crate::MAX_SEQNO)?.is_none(),
        "a key in a zeroed block must miss cleanly, not error",
    );
    // The intact first block is below the punched region: a single lower bound
    // cannot keep it while excluding the zeroed blocks above it, and its rows
    // are superseded by the committed output anyway.
    assert!(
        tree.get(b"k00000", crate::MAX_SEQNO)?.is_none(),
        "the intact-but-superseded first block must not be served",
    );
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the live suffix must be served",
    );
    Ok(())
}

/// The recovery-FAILURE counterpart: a partially punched, sidecar-less SST that
/// also fails whole-file recovery must be set aside, not block-salvaged into an
/// unrestricted output. The cheap pre-salvage probe reads only the FIRST bytes
/// and cannot see a punch that left the first block intact; the salvage walk's
/// dropped all-zero extents must catch it instead.
#[test]
fn repair_sets_aside_a_partially_punched_sidecarless_sst_that_fails_recovery() -> crate::Result<()>
{
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    // A PERSISTENT fault on the FIRST streaming read (the preliminary whole-file
    // hash) fails whole-file recovery and routes repair to the salvage arm; the
    // salvage itself (reading the quarantined copy) runs unfaulted and WOULD
    // succeed, resurrecting the intact-but-superseded first block unrestricted.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a partially punched sidecar-less SST that fails recovery is set aside, \
         not salvaged into an unrestricted output: {report:?}",
    );
    assert_eq!(report.unreadable, 1, "{:?}", report.unreadable_files);
    assert!(
        report
            .unreadable_files
            .first()
            .is_some_and(|(_, reason)| reason.contains("punched extents found during salvage")),
        "the reason names the punched extents the walk found: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// The punch-on-drop reclaim must leave a CLASSIFIABLE hole pattern when
/// individual `punch_hole` calls fail: punching top-down and STOPPING at the
/// first failure guarantees any failure (or crash) leaves intact blocks BELOW
/// the zeroed ones — the irregular signature default repair sets aside. The
/// old bottom-up continue-past-failures order left trailing intact-but-consumed
/// blocks ABOVE a clean zeroed prefix, indistinguishable from a live suffix, so
/// a sidecar-less repair restricted to a bound that resurrected their
/// superseded rows.
#[test]
fn punch_failures_leave_a_classifiable_pattern() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let faultfs = FaultFs::new(memfs.as_ref().clone());
    let injector = faultfs.injector();
    let fs: Arc<dyn Fs> = Arc::new(faultfs);
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // Arm a PERSISTENT punch fault that lets the first two attempts through:
    // top-down with stop-on-first-failure this zeroes only the top two consumed
    // blocks and leaves everything below intact (an irregular, classifiable
    // pattern). Bottom-up with continue-past-failures it would zero the two
    // LOWEST blocks and leave trailing consumed blocks that read as a clean
    // zeroed prefix plus a plausible live suffix.
    let table = recover_sst(sst, &fs)?;
    let committed = table.punch_offset_for(b"k00130")?;
    table.mark_punch_on_drop(committed);
    injector.arm(FaultRule::new(FaultOp::PunchHole, Fault::Error(ErrorKind::Other)).skip(2));
    drop(table);
    injector.clear();

    // Default repair with NO sidecar must classify the pattern as an irregular
    // punch and set the table aside — never restrict to a bound that serves the
    // intact-but-consumed blocks the failed punches left behind.
    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a punch-failure pattern must be set aside, not restricted to a bound \
         that resurrects the unpunched consumed blocks: {report:?}",
    );
    assert_eq!(report.unreadable, 1, "{:?}", report.unreadable_files);
    assert!(
        report
            .unreadable_files
            .first()
            .is_some_and(|(_, reason)| reason.contains("punch failures")),
        "{:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// The restore's raw sidecar capture must apply the same size cap
/// `restrict_bound::read` enforces: an attacker-padded or corrupt oversized
/// sidecar must be classified unreadable (no rescue copy), not trusted into a
/// full-size allocation and re-published verbatim by the rename fallback.
#[test]
fn restore_does_not_republish_an_oversized_sidecar() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, SyncMode};
    use crate::io::ErrorKind;
    use std::io::Write;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    std::fs::write(&sst, b"sst bytes")?;

    let fs = FaultFs::new(StdFs);
    // An oversized "sidecar": larger than any valid header + max key + checksum
    // (+ encryption overhead) encoding.
    let sidecar = crate::restrict_bound::sidecar_path(&sst);
    {
        let mut f = fs.open(
            &sidecar,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(&vec![0x42u8; (usize::from(u16::MAX)) * 2])?;
    }
    let dest = super::quarantine_file(&fs, &tables, &sst, "0", SyncMode::Normal)?;

    // Fault the direct sidecar rename on the way back: with no rescue copy
    // captured (oversized = unreadable), the restore must complete WITHOUT
    // re-publishing the junk at the destination.
    fs.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
            .on_path("restrict-bound")
            .once(),
    );
    super::restore_quarantined(&fs, &dest, &sst, None, SyncMode::Normal)?;
    assert!(fs.exists(&sst)?, "the SST is restored to tables/");
    assert!(
        !fs.exists(&crate::restrict_bound::sidecar_path(&sst))?,
        "an oversized (corrupt) sidecar must not be re-published beside the SST",
    );
    Ok(())
}

/// A flag-dependent set-aside must keep the resurrection knob TWO-WAY: a
/// default (resurrection-off) repair sets an irregularly punched SST aside
/// because its bound is unknowable, but a later resurrection repair must
/// reclaim it from quarantine automatically — no manual file move — and
/// recover its readable region greedily. Unrelated quarantined files (which
/// carry no resurrectable marker) are never reclaimed.
#[test]
fn resurrection_reclaims_an_irregularly_punched_set_aside() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    // Default repair: the irregular punch makes the bound unknowable → set aside.
    let first = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");
    assert_eq!(first.unreadable, 1, "{:?}", first.unreadable_files);

    // Unrelated quarantine content without a marker must survive the reclaim.
    let quarantine = root.join("repair-quarantine");
    let junk = quarantine.join("junk-name");
    {
        use std::io::Write;
        let mut f = fs.open(
            &junk,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(b"not a table")?;
    }

    // Resurrection repair: the marked set-aside is reclaimed and recovered
    // greedily, with no manual step in between.
    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(false, true)?;
    assert_eq!(
        second.recovered, 1,
        "the resurrection repair must reclaim the marked set-aside: {second:?}",
    );
    assert_eq!(second.unreadable, 0, "{:?}", second.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    assert!(
        tree.get(b"k00020", crate::MAX_SEQNO)?.is_none(),
        "a key in a zeroed block still misses cleanly",
    );
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the readable region is served under resurrection",
    );
    drop(tree);

    assert!(
        !fs.exists(&quarantine.join("0"))?,
        "the reclaimed SST must leave quarantine",
    );
    assert!(
        !fs.exists(&quarantine.join("0.resurrectable"))?,
        "the marker is consumed by the reclaim",
    );
    assert!(
        fs.exists(&junk)?,
        "unmarked quarantine content is never reclaimed",
    );
    Ok(())
}

/// The salvage-walk punch guard must scan the WHOLE surrendered extent, not
/// just its opening window: when the physical chain breaks, the walk can
/// surrender the entire remaining data tail as ONE dropped extent whose offset
/// is the first DAMAGED (nonzero) frame, leaving punched blocks further down
/// invisible to a 64-byte probe. With no sidecar and an intact first block,
/// both punch guards would then pass and the salvaged output would publish the
/// consumed records unrestricted, resurrecting superseded data.
#[test]
fn salvage_guard_finds_punched_blocks_deep_in_a_surrendered_extent() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // Punch a MIDDLE data block (not the first), so the file's opening bytes
    // stay intact and the zeroed region sits deep inside the data section —
    // exactly the shape a surrendered tail hides from an opening-window probe.
    let table = recover_sst(sst.clone(), &fs)?;
    let mut handles = Vec::new();
    {
        use crate::table::block_index::BlockIndex;
        for handle in table.block_index.iter() {
            let handle = handle?;
            handles.push((handle.offset().0, handle.size()));
        }
    }
    assert!(handles.len() >= 4, "fixture needs several data blocks");
    let (Some(&(mid_off, mid_size)), Some(&(first_off, _))) =
        (handles.get(handles.len() / 2), handles.first())
    else {
        panic!("fixture has >= 4 data blocks, so both lookups resolve");
    };
    drop(table);
    memfs.punch_hole(&sst, mid_off, u64::from(mid_size))?;

    // The whole data section, surrendered as ONE extent starting at its first
    // byte (the shape `salvage_attempt` produces after a broken chain): the
    // opening window is intact data, the punched block is deeper in.
    let dropped = vec![crate::salvage::DroppedBlock {
        offset: first_off,
        section: b"data".to_vec(),
        reason: crate::salvage::DropReason::HeaderCorrupted("surrendered tail".to_owned()),
        key_range: None,
    }];
    assert!(
        super::dropped_data_extent_is_zeroed(&*fs, &sst, &dropped)?,
        "a punched block deeper inside the surrendered extent must be found",
    );

    // An unpunched source must still not false-positive.
    let clean = tables.join("1");
    write_multiblock_sst(&clean, &fs)?;
    assert!(
        !super::dropped_data_extent_is_zeroed(&*fs, &clean, &dropped)?,
        "an unpunched source must never be reported as punched",
    );
    Ok(())
}

/// The standalone out-of-band verify must not condemn a healthy restricted
/// punched SST: with a valid colocated sidecar attesting the committed
/// restriction, the leading zeroed (punched) region is the reclaimed prefix
/// and the walk starts at the live frontier, verifying the suffix clean.
/// WITHOUT the sidecar the zeros stay part of the walk and flag loudly:
/// zeroed-out data on an unrestricted table is destruction, not reclaim.
#[test]
fn verify_sst_file_honors_a_restricted_punched_prefix() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00130", SyncMode::Normal)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00130")?;
    memfs.punch_hole(&sst, 0, punch)?;

    let report = crate::verify::verify_sst_file_with_fs(&*fs, &sst);
    assert!(
        report.is_ok(),
        "a healthy restricted punched SST must verify clean through the \
         out-of-band walk: errors {:?}, warnings {:?}",
        report.errors,
        report.warnings,
    );
    assert!(
        report.blocks_scanned > 0,
        "the live suffix must actually be walked: {report:?}",
    );

    // Without the attesting sidecar the zeroed region must flag loudly.
    crate::restrict_bound::remove(&*fs, &sst, SyncMode::Normal);
    let report = crate::verify::verify_sst_file_with_fs(&*fs, &sst);
    assert!(
        !report.is_ok(),
        "leading zeros without a restriction sidecar are destroyed data and \
         must fail verification: {report:?}",
    );
    Ok(())
}

/// The frontier derive must clear the LAST punched extent, not stop at the
/// first nonzero byte: the reclaim punches top-down and stops at its first
/// failure, so a partial reclaim leaves intact consumed blocks BELOW the holes
/// it did punch. Anchoring at the first nonzero byte would put those holes
/// back inside the walk and condemn a healthy sidecar-backed SST as corrupt.
#[test]
fn verify_sst_file_clears_partial_top_down_holes() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00130", SyncMode::Normal)?;

    // A PARTIAL top-down reclaim: punch the consumed blocks nearest the bound
    // and leave the lowest ones intact (the shape a failed / crashed reclaim
    // leaves, since the pass stops at its first failure).
    let table = recover_sst(sst.clone(), &fs)?;
    let committed = table.punch_offset_for(b"k00130")?;
    let mut consumed = Vec::new();
    {
        use crate::table::block_index::BlockIndex;
        for handle in table.block_index.iter() {
            let handle = handle?;
            if handle.offset().0 < committed {
                consumed.push((handle.offset().0, handle.size()));
            }
        }
    }
    assert!(consumed.len() >= 3, "fixture needs several consumed blocks");
    drop(table);
    for &(off, size) in consumed.iter().skip(1) {
        memfs.punch_hole(&sst, off, u64::from(size))?;
    }

    let report = crate::verify::verify_sst_file_with_fs(&*fs, &sst);
    assert!(
        report.is_ok(),
        "a partially reclaimed sidecar-backed SST must verify clean: errors \
         {:?}, warnings {:?}",
        report.errors,
        report.warnings,
    );
    assert!(
        report.blocks_scanned > 0,
        "the live suffix must still be walked: {report:?}",
    );
    Ok(())
}

/// A reclaim whose post-rename step fails must not leave the SST in `tables/`
/// unreferenced: the previously installed manifest omits it, so a caller that
/// responds to the failed repair by simply REOPENING the tree lets orphan
/// cleanup delete the only recovered copy. Every failure after the rename must
/// roll the file back into quarantine, marker intact, where the next
/// resurrection repair rediscovers it.
#[test]
fn reclaim_rolls_back_to_quarantine_when_a_post_rename_step_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    // Default repair sets the irregularly punched SST aside with a marker.
    let first = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");

    // Fault the reclaim's first destination-directory sync (the only tables/
    // sync before the scan starts on a resurrection run): the rename back into
    // tables/ has already happened by then.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_resurrection(true, true);
    assert!(second.is_err(), "the faulted reclaim sync must surface");

    let quarantine = root.join("repair-quarantine");
    assert!(
        !fs.exists(&tables.join("0"))?,
        "the reclaimed SST must not be left in tables/ where a plain reopen's \
         orphan cleanup would delete it",
    );
    assert!(
        fs.exists(&quarantine.join("0"))?,
        "the SST must be rolled back into quarantine",
    );
    assert!(
        fs.exists(&quarantine.join("0.resurrectable"))?,
        "the marker must survive the rollback so a retry rediscovers the file",
    );

    // The retry (no fault) reclaims and recovers it.
    let third = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(third.recovered, 1, "{third:?}");
    Ok(())
}

/// The pre-salvage first-bytes guard's set-aside (a FULLY punched, sidecar-less
/// SST that fails whole-file recovery) is two-way as well: default repair sets
/// it aside marked, and a later resurrection repair reclaims and recovers its
/// readable region.
#[test]
fn resurrection_reclaims_a_fully_punched_set_aside() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00130")?;
    memfs.punch_hole(&sst, 0, punch)?;

    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let first = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");

    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(
        second.recovered, 1,
        "the resurrection repair must reclaim the marked set-aside: {second:?}",
    );

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the readable region is served under resurrection",
    );
    Ok(())
}

/// The recovery-failure arm's flag-dependent set-asides are two-way too: a
/// partially punched, sidecar-less SST whose whole-file recovery also failed is
/// set aside by default repair, and a later resurrection repair reclaims and
/// recovers it (the transient recovery fault is gone on the re-run).
#[test]
fn resurrection_reclaims_a_punched_set_aside_from_the_salvage_arm() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    // One-shot read fault fails the whole-file hash → recovery-failure arm →
    // salvage detects the punched dropped extents → set aside (bound lost).
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let first = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");

    // Resurrection repair on the clean fs reclaims the marked original and
    // recovers its readable region.
    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(
        second.recovered, 1,
        "the resurrection repair must reclaim the marked set-aside: {second:?}",
    );

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the readable region is served under resurrection",
    );
    Ok(())
}

/// Writing the `.restrict-bound` sidecar must NEVER mutate the SST — that is the
/// point of a separate file: the manifest's whole-file checksum for the SST stays
/// valid across the write, so a crash between the sidecar write and the manifest
/// commit can never make a scrub see the SST as corrupt or a checkpoint hard-link
/// modified bytes under a stale digest (#64).
#[test]
fn writing_the_sidecar_does_not_mutate_the_sst() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::table::Writer;
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    // Absolute so the MemFs directory key matches what `Writer::new` writes (it
    // rewrites through `std::path::absolute`, prepending the drive on Windows).
    let base = std::path::absolute("/d")?;
    fs.create_dir_all(&base)?;
    let sst = base.join("0");
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    let before = compute_table_checksum(&*fs, &sst)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00030", crate::fs::SyncMode::Normal)?;
    let after = compute_table_checksum(&*fs, &sst)?;
    assert_eq!(
        before, after,
        "publishing the sidecar must leave the SST byte-identical",
    );
    Ok(())
}

/// A restricted, punched SST whose LIVE suffix is ALSO corrupt (a rare double
/// failure) is recovered, not stranded: salvage recovers the readable suffix
/// blocks (dropping the zeroed prefix and the corrupt straddling block), then the
/// result is reopened restricted to the recorded bound and its sidecar is
/// re-written, so the live suffix survives while nothing below the bound is
/// resurrected (#65). The corrupt straddling block's keys are the only casualty:
/// that is the price of the corruption, not of the restriction.
#[test]
fn repair_salvages_a_restricted_punched_sst_with_a_corrupt_suffix() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..256u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Record the bound, punch the consumed prefix, then CORRUPT the first live
    // (straddling) block so the restricted view's verify flags it for salvage.
    let bound = b"k00130".to_vec();
    let punch_offset = {
        let table = recover_sst(sst.clone(), &fs)?;
        crate::restrict_bound::write(&*fs, &sst, None, 0, &bound, crate::fs::SyncMode::Normal)?;
        let off = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, off)?;
        off
    };
    {
        let mut f = fs.open(&sst, &FsOpenOptions::new().write(true))?;
        f.seek(SeekFrom::Start(punch_offset + 20))?;
        f.write_all(&[0xFFu8])?;
        f.sync_all()?;
    }

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the restricted SST's readable live suffix is salvaged, not stranded: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);
    // The salvaged replacement re-records its bound so a later manifest-loss
    // repair honors it (the fresh file is unpunched).
    assert!(
        crate::restrict_bound::exists(&*fs, &sst)?,
        "the salvaged restricted SST re-writes its `.restrict-bound` sidecar",
    );

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // No sub-bound key is resurrected, whatever the corrupt block cost above it.
    for i in 0..130u32 {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_none(),
            "sub-bound key {} must never be resurrected",
            String::from_utf8_lossy(&key),
        );
    }
    // The live suffix well above the dropped straddling block survives.
    for i in [200u32, 255] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_some(),
            "live suffix key {} must survive salvage",
            String::from_utf8_lossy(&key),
        );
    }
    Ok(())
}

/// Builds a punched, restricted SST at `tables/0` whose first LIVE (straddling)
/// block is corrupt, the shape that drives repair through salvage and forces it to
/// re-impose the restriction on the salvaged output. Returns the state-sharing
/// `MemFs` and the absolute root. The re-restriction-fault tests below arm a fault
/// on the sidecar write and assert the quarantined original is restored.
#[cfg(feature = "std")]
fn build_restricted_corrupt_sst()
-> crate::Result<(std::sync::Arc<crate::fs::MemFs>, std::path::PathBuf)> {
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::table::Writer;
    use crate::{InternalValue, ValueType};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..256u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    let bound = b"k00130".to_vec();
    let punch_offset = {
        let table = recover_sst(sst.clone(), &fs)?;
        crate::restrict_bound::write(&*fs, &sst, None, 0, &bound, crate::fs::SyncMode::Normal)?;
        let off = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, off)?;
        off
    };
    {
        let mut f = fs.open(&sst, &FsOpenOptions::new().write(true))?;
        f.seek(SeekFrom::Start(punch_offset + 20))?;
        f.write_all(&[0xFFu8])?;
        f.sync_all()?;
    }
    Ok((memfs, root))
}

/// A fault (of the given kind) on the RE-RESTRICTION sidecar write (the SECOND
/// `.restrict-bound` open; the first is repair reading the recorded bound) must
/// restore the quarantined original and propagate, never leave the unpunched,
/// sidecar-less salvaged replacement in place: a later recovery would open THAT
/// unrestricted and resurrect the sub-bound rows. This must hold for a PERSISTENT
/// failure (ENOSPC-class) as well as a transient one, since the retry cannot
/// re-derive the bound from a fresh unpunched output.
#[cfg(feature = "std")]
fn assert_re_restriction_fault_restores_the_original(
    fault_kind: crate::io::ErrorKind,
) -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::{Config, SequenceNumberCounter};

    let (memfs, root) = build_restricted_corrupt_sst()?;
    let sst = root.join("tables").join("0");

    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(fault_kind))
            .on_path("restrict-bound")
            .skip(1)
            .once(),
    );

    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true);
    assert!(
        matches!(&result, Err(crate::Error::Io(e)) if e.kind() == fault_kind),
        "the re-restriction fault must propagate, got {result:?}",
    );

    // The distinguishing evidence of the restore: NO stranded original is left in
    // quarantine. Without the restore the salvaged replacement stays at the table
    // path and the punched original is orphaned in `repair-quarantine/0` forever, a
    // copy no retry under `tables/` can ever reach.
    assert!(
        !memfs.exists(&root.join("repair-quarantine").join("0"))?,
        "the fault must restore the original, leaving nothing in quarantine",
    );
    assert!(
        memfs.exists(&sst)?,
        "the original SST is back at its table path after the restore",
    );
    Ok(())
}

/// A TRANSIENT fault re-imposing the restriction on a salvaged output restores the
/// quarantined original and propagates for retry. The retry then recovers the
/// table restricted, with no sub-bound resurrection.
#[test]
fn repair_restores_the_original_when_re_restriction_faults_transiently() -> crate::Result<()> {
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    assert_re_restriction_fault_restores_the_original(ErrorKind::Interrupted)?;

    // The retry (no fault) recovers the table restricted: no sub-bound resurrection.
    let (memfs, root) = build_restricted_corrupt_sst()?;
    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the retry recovers the table: {report:?}"
    );
    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    assert!(
        tree.get(b"k00000", crate::MAX_SEQNO)?.is_none(),
        "no sub-bound key resurrects after the retry",
    );
    Ok(())
}

/// The PERSISTENT counterpart: an ENOSPC-class (non-transient) fault re-imposing
/// the restriction must ALSO restore the quarantined original, not only transient
/// ones. Recovery cannot re-derive the bound from the fresh unpunched salvaged
/// output, so leaving it in place would let a retry install it UNRESTRICTED and
/// resurrect the sub-bound rows.
#[test]
fn repair_restores_the_original_when_re_restriction_faults_persistently() -> crate::Result<()> {
    use crate::io::ErrorKind;

    assert_re_restriction_fault_restores_the_original(ErrorKind::Other)
}

/// A PUNCHED SST with no trustworthy bound (missing sidecar) that ALSO fails
/// whole-file recovery must be set aside, NOT block-salvaged into an unrestricted
/// output: recovery leaves no `Table` to derive a geometry bound from, and salvage
/// re-emits the straddling block's sub-bound rows with nothing to restrict them.
/// Fail closed on the ambiguity.
#[test]
fn repair_sets_aside_a_punched_sidecarless_sst_that_fails_recovery() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..256u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // Punch the consumed prefix (zeroes block 0) WITHOUT recording a sidecar. The
    // tail (meta/index) stays intact, so block salvage still succeeds; only the
    // whole-file recovery is made to fail below.
    let punch_offset = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00130")?;
    memfs.punch_hole(&sst, 0, punch_offset)?;

    // A PERSISTENT fault on the FIRST streaming read (the preliminary whole-file
    // hash) fails whole-file recovery and routes repair to the salvage arm, while
    // salvage (reading the quarantined copy) and the punch probe run unfaulted. So
    // salvage WOULD succeed and, without the fail-closed guard, install the
    // unpunched output UNRESTRICTED, resurrecting the straddling block's sub-bound
    // rows. The guard sets it aside instead.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 0,
        "a punched sidecar-less SST that fails recovery is set aside, not salvaged \
         into an unrestricted output: {report:?}",
    );
    assert_eq!(report.unreadable, 1, "{:?}", report.unreadable_files);
    assert!(
        report
            .unreadable_files
            .first()
            .is_some_and(|(_, reason)| reason.contains("no recoverable restriction bound")),
        "the reason names the unrecoverable punched bound: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// Quarantining a RESTRICTED SST must carry its `.restrict-bound` sidecar along:
/// left behind in `tables/`, the next open's orphan sweep (the rebuilt manifest no
/// longer names the id) deletes it, permanently stranding the quarantined punched
/// file from its exact recovery boundary. `restore_quarantined` moves it back the
/// same way.
#[test]
fn quarantine_moves_the_restriction_sidecar_with_the_sst() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00050", SyncMode::Normal)?;

    let src_sidecar = crate::restrict_bound::sidecar_path(&sst);
    assert!(
        fs.exists(&src_sidecar)?,
        "precondition: the sidecar was written"
    );

    // Quarantine the SST; its sidecar must follow it out of `tables/`.
    let dest = super::quarantine_file(&*fs, &tables, &sst, "0", SyncMode::Normal)?;
    let dest_sidecar = crate::restrict_bound::sidecar_path(&dest);
    assert!(
        fs.exists(&dest_sidecar)?,
        "the sidecar must move into repair-quarantine/ next to its SST",
    );
    assert!(
        !fs.exists(&src_sidecar)?,
        "no orphaned sidecar may be left in tables/ for the orphan sweep to delete",
    );
    assert!(!fs.exists(&sst)?, "the SST itself moved out of tables/");

    // Restoring the original brings the sidecar back with it.
    super::restore_quarantined(&*fs, &dest, &sst, None, SyncMode::Normal)?;
    assert!(fs.exists(&sst)?, "the SST is restored to tables/");
    assert!(
        fs.exists(&src_sidecar)?,
        "the sidecar is restored alongside the SST",
    );
    assert!(
        !fs.exists(&dest_sidecar)?,
        "no sidecar may be left orphaned in repair-quarantine/ after restore",
    );
    Ok(())
}

/// A failure while moving the companion sidecar (after the SST itself already
/// moved into `repair-quarantine/`) must roll the SST back under `tables/`:
/// propagating with the table stranded in quarantine means the retried repair no
/// longer discovers it and installs a manifest that omits it — permanent loss
/// from a one-shot rename fault. The rollback leaves both files exactly where a
/// retry can find and re-quarantine them.
#[test]
fn quarantine_rolls_back_the_sst_when_the_sidecar_move_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    std::fs::write(&sst, b"sst bytes")?;

    let fs = FaultFs::new(StdFs);
    crate::restrict_bound::write(&fs, &sst, None, 0, b"k00050", SyncMode::Normal)?;
    let src_sidecar = crate::restrict_bound::sidecar_path(&sst);
    assert!(fs.exists(&src_sidecar)?, "precondition: sidecar written");

    // Fault the SIDECAR's rename only: `Rename` matches the destination path,
    // and only the sidecar's destination contains "restrict-bound" (the SST
    // moves to a bare numeric name). The SST rename has succeeded by then.
    fs.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Interrupted))
            .on_path("restrict-bound"),
    );

    assert!(
        quarantine_file(&fs, &tables, &sst, "0", SyncMode::Normal).is_err(),
        "the sidecar rename fault must surface",
    );
    assert!(
        fs.exists(&sst)?,
        "the SST must be rolled back under tables/ so a retry rediscovers it",
    );
    assert!(
        fs.exists(&src_sidecar)?,
        "the sidecar must stay beside the SST under tables/",
    );
    let quarantine = dir.path().join("repair-quarantine");
    assert!(
        !fs.exists(&quarantine.join("0"))?,
        "no stranded SST may be left in repair-quarantine/",
    );
    Ok(())
}

/// A failed sidecar rename during `restore_quarantined` must not strand the
/// exact bound in quarantine (where the retried repair never looks, silently
/// degrading the restored SST to the lossy geometry fallback): the restore
/// falls back to RE-PUBLISHING the sidecar bytes captured before the move, so
/// it completes and a retry recovers the EXACT bound. The SST itself is never
/// rolled back into quarantine — a pair stranded there is invisible to the
/// retry entirely.
#[test]
fn restore_republishes_the_sidecar_when_its_rename_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    std::fs::write(&sst, b"sst bytes")?;

    let fs = FaultFs::new(StdFs);
    crate::restrict_bound::write(&fs, &sst, None, 0, b"k00050", SyncMode::Normal)?;
    let dest = super::quarantine_file(&fs, &tables, &sst, "0", SyncMode::Normal)?;
    let dest_sidecar = crate::restrict_bound::sidecar_path(&dest);
    assert!(
        fs.exists(&dest_sidecar)?,
        "precondition: sidecar quarantined"
    );

    // Fault the sidecar's direct rename on the way BACK (`Rename` matches the
    // destination path; only the sidecar's destination contains
    // "restrict-bound"). `.once()` so the fallback's own tmp → final rename
    // succeeds.
    fs.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
            .on_path("restrict-bound")
            .once(),
    );

    super::restore_quarantined(&fs, &dest, &sst, None, SyncMode::Normal)?;
    assert!(fs.exists(&sst)?, "the SST is restored to tables/");
    let src_sidecar = crate::restrict_bound::sidecar_path(&sst);
    assert!(
        fs.exists(&src_sidecar)?,
        "the sidecar must be re-published beside the restored SST",
    );
    match crate::restrict_bound::read(&fs, &sst, None)? {
        crate::restrict_bound::SidecarRead::Present(id, bound) => {
            assert_eq!(id, 0);
            assert_eq!(bound, b"k00050", "the re-published bound is byte-intact");
        }
        _ => panic!("the re-published sidecar must decode to the exact bound"),
    }
    assert!(
        !fs.exists(&dest_sidecar)?,
        "no stale sidecar may linger in quarantine after the re-publish",
    );
    Ok(())
}

/// Asserts a punched SST at `tables/0` was recovered RESTRICTED to the exact
/// sidecar `bound` by default repair (resurrection off): the table joins the
/// manifest, nothing is set aside, and a key is served IFF it is at or above the
/// bound. Shared by the honor-the-bound tests (a valid sidecar the punch does not
/// fully back).
fn assert_recovered_restricted_to(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    root: &std::path::Path,
    bound: &[u8],
) -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let report = Config::new(
        root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the SST is recovered restricted to its bound, not set aside: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    for i in 0..256u32 {
        let key = format!("k{i:05}").into_bytes();
        let served = tree.get(&key, crate::MAX_SEQNO)?.is_some();
        assert_eq!(
            served,
            key.as_slice() >= bound,
            "key {key:?} served={served}; expected served == (key >= {bound:?})",
        );
    }
    Ok(())
}

/// A `.restrict-bound` sidecar whose bound reaches PAST the actually-punched
/// extent is not fully backed by the punch (an earlier slice punched `[0, B1)`
/// but a later, larger bound `B2 > B1` never committed, so `[B1, B2)` stays live).
/// With resurrection off, repair honors the recorded bound: it restricts to `B2`,
/// dropping the ambiguous prefix (including the live `[B1, B2)`) rather than
/// resurrecting the superseded sub-`B1` rows an unrestricted open would expose.
/// The live suffix above `B2` is always kept.
#[test]
fn repair_restricts_a_punched_sst_whose_sidecar_bound_overshoots_the_punch() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;

    // Punch `[0, punch(k00050))`, but publish a LARGER sidecar bound `k00130` (as
    // if a later slice's install never landed).
    let sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00130", crate::fs::SyncMode::Normal)?;

    assert_recovered_restricted_to(&memfs, &root, b"k00130")
}

/// A VALID `.restrict-bound` sidecar for this id over an UNPUNCHED SST denotes a
/// COMMITTED restriction whose punch had not yet run: tight-space writes the
/// sidecar STRICTLY AFTER the slice's version install commits, so an aborted
/// slice never leaves one. Repair honors the recorded bound, restricting to it
/// and dropping the prefix (the committed output covers it) rather than
/// resurrecting a superseded sub-bound row. The live suffix is kept.
#[test]
fn repair_honors_a_valid_sidecar_over_an_unpunched_sst() -> crate::Result<()> {
    let (memfs, root) = build_unpunched_sidecar_sst(b"k00130")?;
    assert_recovered_restricted_to(&memfs, &root, b"k00130")
}

/// Builds `tables/0`: a multi-block SST with a VALID `.restrict-bound` sidecar
/// for its own id at `bound`, but NO physical punch — the post-install /
/// pre-punch crash state. Returns the `MemFs` and its absolute root.
fn build_unpunched_sidecar_sst(
    bound: &[u8],
) -> crate::Result<(std::sync::Arc<crate::fs::MemFs>, std::path::PathBuf)> {
    use crate::fs::{Fs, MemFs};
    use crate::table::Writer;
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..256u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    crate::restrict_bound::write(&*fs, &sst, None, 0, bound, crate::fs::SyncMode::Normal)?;

    Ok((memfs, root))
}

/// A committed restriction is honored REGARDLESS of the resurrection flag. Because
/// a valid sidecar over an unpunched SST is provably committed (written strictly
/// after the install), enabling resurrection does NOT reopen the whole table: the
/// flag governs LOST tombstones / restrictions, and this restriction is neither
/// lost nor ambiguous. Repair still restricts to the recorded bound, so no
/// superseded sub-bound row is served. Under the pre-`commit-then-mark` ordering
/// the sidecar could outlive an uncommitted restriction, so resurrection kept the
/// whole table (serving `k00000`); this test pins the committed-honor behaviour.
#[test]
fn repair_with_resurrection_honors_a_committed_unpunched_sidecar() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let (memfs, root) = build_unpunched_sidecar_sst(b"k00130")?;

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(
        report.recovered, 1,
        "the committed restriction is recovered restricted, not set aside: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // The bound is honored even with resurrection ON: keys below it stay dropped,
    // the live suffix at/above it is served.
    assert!(
        tree.get(b"k00000", crate::MAX_SEQNO)?.is_none(),
        "a committed restriction must not be reopened whole by the resurrection flag",
    );
    assert!(
        tree.get(b"k00129", crate::MAX_SEQNO)?.is_none(),
        "the row just below the bound stays dropped",
    );
    assert!(
        tree.get(b"k00130", crate::MAX_SEQNO)?.is_some(),
        "the bound key is served",
    );
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the live suffix is served",
    );
    Ok(())
}

/// Asserts a punched SST at `tables/0` under `memfs` was recovered RESTRICTED by
/// default repair (resurrection off): the table joins the rebuilt manifest,
/// nothing is set aside, no key below the punch (a superseded prefix row) is
/// resurrected, and the live suffix survives. Shared by the no-exact-bound
/// punched-SST tests, which resolve the bound from the punch geometry. The build
/// helper punches `[0, punch(k00050))`, so the conservative derived bound sits at
/// or just above `k00050`.
fn assert_punched_sst_recovered_restricted(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    root: &std::path::Path,
) -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let report = Config::new(
        root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the punched SST is recovered restricted, not set aside: {report:?}",
    );
    assert_eq!(
        report.unreadable, 0,
        "nothing is set aside: {:?}",
        report.unreadable_files,
    );

    let tree = Config::new(
        root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // No superseded prefix row (below the k00050 punch) is resurrected.
    for i in [0u32, 10, 49] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_none(),
            "key {key:?} is below the punch and must NOT be resurrected",
        );
    }
    // The live suffix survives (the derived bound sits just above the punch, so
    // keys well above it are served).
    for i in [100u32, 200, 255] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_some(),
            "live-suffix key {key:?} must be served",
        );
    }
    Ok(())
}

/// A punched SST with NO `.restrict-bound` sidecar (a legacy punched SST predating
/// sidecars, or one whose sidecar was lost) has no exact bound. With resurrection
/// off, repair derives a conservative bound from the punch geometry and recovers
/// the table restricted: the live suffix survives and no superseded prefix row is
/// resurrected. It is never opened unrestricted (which would resurrect the prefix)
/// nor set aside (which would discard the live suffix).
#[test]
fn repair_restricts_a_punched_sst_with_no_sidecar() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    // Punch the prefix but publish NO sidecar.
    let _sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;

    assert_punched_sst_recovered_restricted(&memfs, &root)
}

/// A punched SST whose `.restrict-bound` sidecar reads back MALFORMED (a flipped
/// byte fails its checksum) has no trustworthy exact bound. With resurrection off,
/// repair derives a conservative bound from the punch geometry and recovers the
/// table restricted, rather than routing it through the generic salvage path
/// (which would rewrite it unpunched and re-emit the superseded prefix rows).
#[test]
fn repair_restricts_a_punched_sst_with_a_corrupt_sidecar() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;

    // Publish a valid, punch-backed sidecar, then flip its first byte so it reads
    // back with a mismatched checksum (Corrupt).
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00050", crate::fs::SyncMode::Normal)?;
    let sidecar = crate::restrict_bound::sidecar_path(&sst);
    {
        let mut buf = Vec::new();
        fs.open(&sidecar, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut buf)?;
        if let Some(b) = buf.first_mut() {
            *b ^= 0xFF;
        }
        let mut f = fs.open(&sidecar, &FsOpenOptions::new().write(true))?;
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }

    assert_punched_sst_recovered_restricted(&memfs, &root)
}

/// A punched SST whose `.restrict-bound` sidecar binds a DIFFERENT table id (a
/// stale sidecar left by a reused id) does not authenticate this SST's bound, so
/// there is no trustworthy exact bound. With resurrection off, repair derives a
/// conservative bound from the punch geometry and recovers the table restricted,
/// rather than opening the punched SST whole (which would resurrect its prefix).
#[test]
fn repair_restricts_a_punched_sst_with_a_mismatched_sidecar_id() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;

    // A bound that WOULD be trustworthy (fully punch-backed) but recorded under the
    // WRONG table id, so it never authenticates this SST (named "0", id 0).
    crate::restrict_bound::write(
        &*fs,
        &sst,
        None,
        999,
        b"k00050",
        crate::fs::SyncMode::Normal,
    )?;

    assert_punched_sst_recovered_restricted(&memfs, &root)
}

/// A punched SST with no exact bound, recovered with resurrection ENABLED, keeps
/// its whole readable region instead of restricting: the live suffix survives and
/// the readable prefix keys the conservative derive would have dropped are served
/// again. It is still recovered into a valid tree, never set aside.
#[test]
fn repair_with_resurrection_keeps_a_punched_sst_unrestricted() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    // Punch `[0, punch(k00050))`, publish NO sidecar: no exact bound survives.
    let _sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(
        report.recovered, 1,
        "resurrection recovers the punched SST unrestricted, not set aside: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    // The live suffix survives, and at least one key the conservative derive drops
    // (the readable straddling block just above the punch) is resurrected, so the
    // resurrection view serves strictly more than the default restricted one.
    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the live suffix must be served under resurrection",
    );
    let resurrected = (50u32..100)
        .filter_map(|i| {
            let key = format!("k{i:05}").into_bytes();
            tree.get(&key, crate::MAX_SEQNO).ok().flatten().map(|_| i)
        })
        .count();
    assert!(
        resurrected > 0,
        "resurrection must serve readable keys the conservative derive would drop",
    );
    Ok(())
}

/// A punched SST with no trustworthy sidecar, recovered with resurrection ON but
/// salvage OFF, must NOT be opened unrestricted: its reclaimed prefix is zeroed
/// block frames, so a read routed to one of them would fail with block corruption
/// after a supposedly successful repair. Resurrection restricts to the first
/// readable block's key (keeping the whole straddling block), so a read below the
/// frontier misses cleanly while the live suffix is served.
#[test]
fn repair_with_resurrection_but_no_salvage_does_not_expose_punched_blocks() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes (see the
    // sibling tests for the Windows drive-relative rationale).
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    // Punch `[0, punch(k00050))`, publish NO sidecar: no exact bound survives.
    let _sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;

    // salvage OFF (no rewrite to drop the zeroed blocks), resurrection ON.
    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(false, true)?;
    assert_eq!(
        report.recovered, 1,
        "the punched SST is recovered: {report:?}"
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    // A read in the punched prefix must MISS cleanly. Unrestricted, this get would
    // route to a zeroed block and error (the `?` would propagate it, failing the
    // test) — the restriction makes it miss below the frontier instead.
    assert!(
        tree.get(b"k00000", crate::MAX_SEQNO)?.is_none(),
        "a key in the punched prefix must miss cleanly, not error on a zeroed block",
    );
    assert!(
        tree.get(b"k00255", crate::MAX_SEQNO)?.is_some(),
        "the live suffix must be served",
    );
    Ok(())
}

/// Repair must PROPAGATE a transient `.restrict-bound` sidecar read on a punched
/// SST, not silently open it unrestricted (which would expose the zeroed prefix).
/// A one-shot `Interrupted` opening the sidecar is retryable, so `read` returns an
/// I/O error that repair classifies as transient and re-raises, rather than
/// installing a manifest without the restriction.
#[test]
fn repair_propagates_a_transient_restrict_bound_read() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let membase: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    membase.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &membase)?;

    // Record a mid-block bound in the sidecar, then punch the consumed prefix.
    let bound = b"k00130".to_vec();
    {
        let table = recover_sst(sst.clone(), &membase)?;
        crate::restrict_bound::write(
            &*membase,
            &sst,
            None,
            0,
            &bound,
            crate::fs::SyncMode::Normal,
        )?;
        let punch = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, punch)?;
    }

    // Fault the sidecar OPEN with a TRANSIENT kind.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted))
            .on_path("restrict-bound"),
    );

    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true);
    assert!(
        result.is_err(),
        "a transient sidecar read on a punched SST must propagate, not silently \
         open it unrestricted: {result:?}",
    );
    Ok(())
}

/// A PERSISTENT `.restrict-bound` sidecar read error (EIO / `PermissionDenied`,
/// outside the transient allowlist) on a punched SST leaves no trustworthy exact
/// bound. With resurrection off, repair derives a conservative bound from the
/// punch geometry and recovers the table restricted, rather than routing it
/// through the generic salvage path (which would rewrite it unpunched and re-emit
/// the superseded prefix rows).
#[test]
fn repair_restricts_a_punched_sst_on_a_persistent_sidecar_read() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let membase: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes: the
    // writer rewrites its output path through `std::path::absolute`, which on
    // Windows prepends the current drive (`/db` -> `D:\db`). Building the root the
    // same way keeps `create_dir_all` and the writer agreed on one namespace.
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    membase.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &membase)?;

    // A valid bound plus a real punch of the consumed prefix.
    let bound = b"k00130".to_vec();
    {
        let table = recover_sst(sst.clone(), &membase)?;
        crate::restrict_bound::write(
            &*membase,
            &sst,
            None,
            0,
            &bound,
            crate::fs::SyncMode::Normal,
        )?;
        let punch = table.punch_offset_for(&bound)?;
        memfs.punch_hole(&sst, 0, punch)?;
    }

    // Fault the sidecar OPEN with a PERSISTENT kind (not in the retry allowlist).
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("restrict-bound"),
    );

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(
        report.recovered, 1,
        "the punched SST is recovered restricted, not set aside: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    // The punch reclaimed `[0, punch(k00130))`, so the derived bound sits at or
    // just above k00130: no key below the punch is resurrected, and the live
    // suffix well above it survives.
    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    for i in [0u32, 50, 129] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_none(),
            "key {key:?} is below the punch and must NOT be resurrected",
        );
    }
    for i in [150u32, 200, 255] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_some(),
            "live-suffix key {key:?} must be served",
        );
    }
    Ok(())
}

/// A VALID post-commit sidecar is proof enough of a committed restriction: repair
/// must honor its exact bound WITHOUT first probing the already-dead below-bound
/// prefix. A persistently-unreadable sector in a punched (dead) prefix block must
/// not cost the intact live suffix. Here the sidecar bound `k00050` is valid, the
/// prefix is punched, and the first (dead) data block's positioned read faults
/// persistently. With salvage OFF (the default), reopening straight at the bound
/// recovers the readable suffix; probing the dead prefix first would discard the
/// exact bound and quarantine the whole table.
#[test]
fn repair_honors_a_valid_sidecar_despite_a_persistent_dead_prefix_read() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    // Absolute so the MemFs directory keys match what `Writer::new` writes (it
    // rewrites through `std::path::absolute`, prepending the drive on Windows).
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;

    // Punch `[0, punch(k00050))` and publish a VALID sidecar bound at k00050.
    let sst = build_punched_prefix_sst(&memfs, &fs, &tables)?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00050", crate::fs::SyncMode::Normal)?;

    // Fault the FIRST data block's positioned read (offset 0, a dead below-bound
    // block) with a PERSISTENT kind. Only the dead-prefix probe and geometry
    // fallback read offset 0; the suffix digest reads strictly above the punch.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault
        .injector()
        .arm(FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other)).at_offset(0));

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(false)?;
    assert_eq!(
        report.recovered, 1,
        "a valid sidecar must be honored despite an unreadable dead prefix, not \
         quarantined: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    let tree = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .open()?;
    for i in 0..256u32 {
        let key = format!("k{i:05}").into_bytes();
        let served = tree.get(&key, crate::MAX_SEQNO)?.is_some();
        assert_eq!(
            served,
            key.as_slice() >= b"k00050".as_slice(),
            "key {key:?} served={served}; expected served == (key >= k00050)",
        );
    }
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
    let decision = verify_keep_decision(&config, &faulting, &sst, &table, false);
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

/// With resurrection ENABLED, the same corrupt-delete-bitmap SST is recovered
/// instead of excluded: its bitmap cannot be authenticated, so the rows are
/// re-emitted live, bringing the deleted rows back. The tree opens with the whole
/// table present.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_resurrection_recovers_a_corrupt_delete_bitmap_sst() -> crate::Result<()> {
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

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

    // Corrupt the delete_bitmap section so its content cannot be authenticated.
    let (pos, len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let entry = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"delete_bitmap")
            .ok_or(crate::Error::Unrecoverable)?;
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
    .repair_with_resurrection(true, true)?;
    assert_eq!(
        report.recovered, 1,
        "resurrection recovers the table instead of excluding it: {report:?}",
    );
    assert_eq!(report.unreadable, 0, "{:?}", report.unreadable_files);

    // Every row is live, including the ones the lost bitmap had deleted.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in [5u32, 50, 100, 150] {
        let key = format!("k{i:05}").into_bytes();
        assert!(
            tree.get(&key, crate::MAX_SEQNO)?.is_some(),
            "key {key:?} must be served under resurrection",
        );
    }
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
    // The original SST is set aside for inspection, not left in `tables/`
    // (where the next open's orphan cleanup would delete it).
    let quarantine = dir.path().join("repair-quarantine").join("0");
    assert!(
        quarantine.exists(),
        "the original SST must be set aside, got {:?}",
        report.unreadable_files,
    );
    assert!(
        !tables.join("0").exists(),
        "the corrupt original must not stay in tables/",
    );
    Ok(())
}

/// The resurrection-on counterpart of
/// [`repair_with_salvage_quarantines_a_toc_hidden_range_tombstone_sst`], pinning
/// the boundary between POLICY and MECHANISM. The resurrection flag governs
/// policy (keep possibly-superseded data vs drop it), but it cannot exceed what
/// salvage can mechanically rebuild: salvage cannot re-emit a range-tombstone
/// table, so a TOC-hidden range tombstone is excluded whatever the flag. Flag-on
/// still routes it through salvage (rather than the pre-emptive concealment
/// gate), salvage reports the table unsalvageable, and the tree opens without
/// it. The outcome is a valid tree with no manual step, not a resurrection: the
/// flag opens the door, but there is no re-emitter behind it for range
/// tombstones. (The flag's observable effect lives on the delete-bitmap path,
/// where salvage CAN re-emit.)
#[test]
fn repair_with_resurrection_still_excludes_a_toc_hidden_range_tombstone_sst() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, SequenceNumberCounter, UserKey, ValueType};
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
    crate::test_forge::forge_section_omitted(&sst, b"range_tombstones")?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_resurrection(true, true)?;
    // Flag-on cannot conjure a range-tombstone re-emitter salvage does not have:
    // the table is excluded, but the tree is still valid and needs no manual step.
    assert_eq!(
        report.recovered, 0,
        "salvage cannot re-emit a range-tombstone table, so flag-on still excludes it: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.unreadable, 1, "{:?}", report.unreadable_files);
    // The exclusion now comes from salvage's mechanical refusal, not the
    // pre-emptive concealment gate: the flag DID route it to salvage.
    assert!(
        report
            .unreadable_files
            .iter()
            .any(|(_, reason)| reason.contains("range tombstones")),
        "flag-on routes to salvage, which reports the range-tombstone table \
         unsalvageable: {:?}",
        report.unreadable_files,
    );
    // A valid tree opens with the table absent (no key of it survives) and no
    // manual recovery step.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert!(
        tree.get(b"k00000", crate::MAX_SEQNO)?.is_none(),
        "the whole excluded table is absent, not partially resurrected",
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
