use super::{
    commit_repair_tmp, compute_table_checksum, compute_table_checksum_with_overrides,
    discard_unreferenced, highest_existing_version_id, repair_tmp_path, toc_may_hide_deletions,
    verify_keep_decision,
};
use crate::fs::StdFs;
use test_log::test;

/// `toc_may_hide_deletions` must PROPAGATE a transient open failure rather than
/// grade it `true` (fail closed): on a table `repair_with_salvage` already found
/// corrupt, a `true` verdict drops the table — losing the healthy
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
/// dropped rather than salvaged into resurrected rows — instead of aborting
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
        "a persistent open failure must fail closed (drop), not propagate: {result:?}",
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

/// A removal is durable only once the directory entry that named the file is on
/// disk. Without that fsync a power loss after repair returns can bring the file
/// back — an orphan the next open must sweep, under a manifest that says the tree
/// is repaired. Fault-inject the directory fsync: a build that never syncs the
/// directory never triggers the fault and wrongly reports the removal durable.
#[test]
fn discard_unreferenced_syncs_the_directory() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let src = tables.join("junk-name");
    std::fs::write(&src, b"orphan")?;

    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).on_path("tables"),
    );

    assert!(
        discard_unreferenced(&fs, &src, SyncMode::Full).is_err(),
        "the directory fsync fault must surface",
    );
    Ok(())
}

/// A file already gone counts as removed: the sweep is idempotent, so a repair
/// retried after a crash mid-sweep finishes it instead of failing on the entries
/// the previous attempt already dealt with.
#[test]
fn discard_unreferenced_treats_a_missing_file_as_done() -> crate::Result<()> {
    use crate::fs::SyncMode;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;

    discard_unreferenced(&StdFs, &tables.join("7"), SyncMode::Normal)?;
    Ok(())
}

/// A table's `.restrict-bound` sidecar must go WITH it. Left behind, the sidecar
/// still names an id: a later run that adopts a different table under that id
/// would reopen it restricted at an unrelated bound and silently hide its prefix.
#[test]
fn discard_unreferenced_removes_the_restriction_sidecar_too() -> crate::Result<()> {
    use crate::fs::{Fs, SyncMode};

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    std::fs::write(&sst, b"table")?;
    let sidecar = crate::restrict_bound::sidecar_path(&sst);
    std::fs::write(&sidecar, b"bound")?;

    discard_unreferenced(&StdFs, &sst, SyncMode::Normal)?;

    assert!(!StdFs.exists(&sst)?, "the table is removed");
    assert!(
        !StdFs.exists(&sidecar)?,
        "its restriction bound must not outlive it",
    );
    Ok(())
}

/// The swap replaces the damaged source with the replacement the committed
/// manifest describes, and carries the replacement's own sidecar onto the final
/// name — a replacement adopted at `{id}` beside the SOURCE's stale sidecar
/// would be reopened at an unrelated bound.
#[test]
fn commit_repair_tmp_replaces_the_source_and_carries_its_sidecar() -> crate::Result<()> {
    use crate::fs::{Fs, SyncMode};

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    std::fs::write(&sst, b"damaged")?;
    std::fs::write(crate::restrict_bound::sidecar_path(&sst), b"stale-bound")?;
    let tmp = repair_tmp_path(&sst);
    std::fs::write(&tmp, b"replacement")?;
    std::fs::write(crate::restrict_bound::sidecar_path(&tmp), b"fresh-bound")?;

    commit_repair_tmp(&StdFs, &tmp, &sst, SyncMode::Normal)?;

    assert_eq!(
        std::fs::read(&sst)?,
        b"replacement",
        "the replacement takes the name the manifest gives it",
    );
    assert!(!StdFs.exists(&tmp)?, "nothing is left under the temp name");
    assert_eq!(
        std::fs::read(crate::restrict_bound::sidecar_path(&sst))?,
        b"fresh-bound",
        "the replacement's own bound replaces the source's",
    );
    Ok(())
}

/// An UNRESTRICTED replacement must clear the source's sidecar. Keeping it would
/// restrict the adopted replacement at a bound that describes bytes that no
/// longer exist, dropping the prefix of a table that has none.
#[test]
fn commit_repair_tmp_clears_a_stale_sidecar_when_the_replacement_has_none() -> crate::Result<()> {
    use crate::fs::{Fs, SyncMode};

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("7");
    std::fs::write(&sst, b"damaged")?;
    let sidecar = crate::restrict_bound::sidecar_path(&sst);
    std::fs::write(&sidecar, b"stale-bound")?;
    let tmp = repair_tmp_path(&sst);
    std::fs::write(&tmp, b"replacement")?;

    commit_repair_tmp(&StdFs, &tmp, &sst, SyncMode::Normal)?;

    assert_eq!(std::fs::read(&sst)?, b"replacement");
    assert!(
        !StdFs.exists(&sidecar)?,
        "an unrestricted replacement must not inherit the source's bound",
    );
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

/// A plain `repair()` (salvage off) must not BLESS an SST whose data block is
/// A bulk-ingested SST stores every entry at LOCAL seqno 0 and keeps its real
/// sequence base in the manifest alone, so a manifest-loss repair cannot know
/// it — which is exactly why such a table is dropped. Its coverage bound is
/// therefore UNKNOWN, and reporting the on-disk local maximum (normally 0) as
/// "the highest seqno it held" would send an operator scoping the possibly
/// superseded history to a point far below the real one.
#[test]
fn repair_reports_an_unknown_seqno_bound_for_a_lost_ingest_offset() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_bulk_ingested(Some(true));
        for i in 0..4u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                b"v",
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
        report.unreadable, 1,
        "the table is dropped: its sequence base is unrecoverable",
    );

    let [(path, first, last, seqno)] = report.lost_coverage.as_slice() else {
        panic!(
            "the dropped table's coverage must be reported, got {:?}",
            report.lost_coverage,
        );
    };
    assert_eq!(path, &sst, "the entry names the dropped file");
    assert_eq!(&**first, b"k00000", "the key range IS knowable");
    assert_eq!(&**last, b"k00003", "the key range IS knowable");
    assert_eq!(
        *seqno, None,
        "the sequence bound is not: publishing the local maximum would scope \
         the affected history far too low",
    );
    Ok(())
}

/// Excluding a table loses what it said about its keys, and older versions of
/// them survive elsewhere: a value it had overwritten, or a key its tombstone
/// had deleted, becomes visible again. No repair can tell those apart without
/// the lost bytes, so the report has to NAME the affected coverage — a caller
/// that only sees "one file was unreadable" cannot tell which keys may now
/// serve a superseded value.
#[test]
fn repair_reports_the_key_coverage_an_excluded_table_lost() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
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
                b"v",
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt a data-block payload byte: metadata still parses, so the lost
    // coverage IS knowable even though the rows are not.
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
    .repair()?;
    assert_eq!(report.unreadable, 1, "the damaged table is excluded");

    let [(path, first, last, seqno)] = report.lost_coverage.as_slice() else {
        panic!(
            "the excluded table's coverage must be reported, got {:?}",
            report.lost_coverage,
        );
    };
    assert_eq!(path, &sst, "the entry names the excluded file");
    assert_eq!(&**first, b"k00000", "first key of the lost range");
    assert_eq!(&**last, b"k00007", "last key of the lost range");
    assert_eq!(
        *seqno,
        Some(8),
        "the highest seqno the lost table held: at or below it, keys in that \
         range may now serve a superseded value",
    );
    Ok(())
}

/// corrupt: whole-file recovery succeeds (the data section is read lazily), and
/// the digest is freshly computed over the already-corrupt bytes, so keeping
/// the table would launder the corruption — the rebuilt manifest counts it as
/// recovered and `verify_integrity` passes while reads of the affected block
/// fail. Block verification runs on EVERY repair; the salvage flag only decides
/// whether a damaged table is rewritten (salvage) or set aside (plain).
#[test]
fn plain_repair_drops_a_table_with_a_corrupt_data_block() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
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
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Flip a payload byte of the sole data block: container, index, and meta
    // stay intact, so whole-file recovery opens the table fine and only a
    // block-level check can see the damage.
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
    .repair()?;
    assert_eq!(
        report.recovered, 0,
        "a table that errors on read must not be blessed into the manifest: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the damaged table is set aside and reported: {:?}",
        report.unreadable_files,
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("salvage"),
        "the reason points the operator at the salvage-enabled repair, got: {reason}",
    );
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
/// original (block-salvage's own read of that source and the reopened salvaged
/// copy are both unaffected).
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
    // block-salvage reads of that source — and the reopen-hash of the clean
    // salvaged copy — unfaulted, so recovery proceeds.
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
/// directly and reopens the salvaged replacement restricted:
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
fn repair_with_salvage_drops_a_bulk_ingested_sst_that_fails_recovery() -> crate::Result<()> {
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

    // Nothing is left in `tables/`: neither the dropped original nor the
    // rejected replacement (our own byproduct, which holds nothing the source
    // did not). Either would be an orphan the next open must sweep.
    let leftovers: Vec<String> = std::fs::read_dir(dir.path().join("tables"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        leftovers.is_empty(),
        "the tables folder must be empty, found {leftovers:?}",
    );
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
/// guard drops it.
#[test]
fn repair_with_salvage_drops_a_legacy_seqno0_sst_that_fails_recovery() -> crate::Result<()> {
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
/// OMITS the table: the damage may be a one-shot read failure, and committing
/// without the table turns it into permanent loss. The repair aborts instead,
/// and — because the salvage reads the source in place — the retry finds that
/// source exactly where it was and re-derives the same salvage from it.
#[test]
fn repair_with_salvage_aborts_on_a_transient_salvage_failure() -> crate::Result<()> {
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

    // Fault the salvage's creation of the replacement with an interrupted-syscall
    // error (the unambiguously transient kind): `try_salvage_table` then fails
    // transiently, with the source untouched.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted))
            .on_path(super::REPAIR_TMP_SUFFIX)
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
        "the source stays at its path so a retry can salvage it",
    );
    assert!(
        !super::repair_tmp_path(&sst).exists(),
        "the half-written replacement must not be left where a retry could adopt it",
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
    let mut params = crate::table::RecoverParams::new(
        path,
        checksum,
        0,
        Arc::clone(fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
    );
    params.descriptor_table = Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8)));
    crate::table::Table::recover(params)
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
    // The intact original is untouched: the abort happens before anything is
    // removed, so a retry starts from the same bytes.
    assert!(
        fs.metadata(&sst).is_ok(),
        "the original file stays in place after the aborted repair",
    );
    Ok(())
}

/// A pending `{id}.heal-attest` sidecar must be PRESERVED by repair, not
/// removed: `Tree::open` recognizes and keeps it (the next scrub reconciles a
/// crashed digest refresh through it). Removing it would strand the healed table
/// under its stale pre-heal digest if the manifest rebuild later failed before
/// committing. The sidecar must still sit next to its SST after a successful
/// repair.
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
        "the heal-attest sidecar stays next to its SST (not removed)",
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
fn repair_drops_a_table_with_an_unrecoverable_ingest_offset() -> crate::Result<()> {
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
    let mut params = crate::table::RecoverParams::new(
        path,
        checksum,
        0,
        std::sync::Arc::clone(fs),
        crate::comparator::default_comparator(),
        std::sync::Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
    );
    params.descriptor_table = Some(std::sync::Arc::new(
        crate::descriptor_table::DescriptorTable::new(8),
    ));
    crate::Table::recover(params)
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
/// safeguard then discards the healthy SST, dropping it from the manifest
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
        "the healthy restricted SST must NOT be dropped: {:?}",
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
    // salvage itself (reading that source) runs unfaulted and WOULD
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

/// The resurrection flag is an INPUT to the run that reads the damaged file, not
/// a state machine spread across runs. On the same inputs, a resurrection repair
/// keeps an irregularly punched SST's readable region; the default repair, which
/// has no bound that separates consumed rows from live ones, drops the table and
/// REMOVES the file. Nothing is stashed for a later run to reconsider, so the
/// operator's choice is the flag they pass, never the order they ran things in.
#[test]
fn resurrection_keeps_an_irregularly_punched_table_the_default_repair_drops() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    // With resurrection: the readable region is kept.
    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    let kept = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(kept.recovered, 1, "{kept:?}");
    assert_eq!(kept.unreadable, 0, "{:?}", kept.unreadable_files);

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

    // Without it, on the same shape: the table is dropped and its file is gone.
    let memfs2 = Arc::new(MemFs::new());
    let fs2: Arc<dyn Fs> = memfs2.clone();
    let root2 = std::path::absolute("/db2")?;
    let tables2 = root2.join("tables");
    fs2.create_dir_all(&tables2)?;
    build_partially_punched_prefix_sst(&memfs2, &fs2, &tables2)?;

    let dropped = Config::new(
        &root2,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs2.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(dropped.recovered, 0, "{dropped:?}");
    assert_eq!(dropped.unreadable, 1, "{:?}", dropped.unreadable_files);
    assert!(
        !fs2.exists(&tables2.join("0"))?,
        "the dropped table's file is removed, not stashed",
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

    // Nor may DESTROYED bytes: a block zeroed by corruption reads exactly like
    // a reclaimed one, and calling it a punch condemns an otherwise salvageable
    // table as bound-lost. The hole is what separates them.
    let destroyed = tables.join("2");
    write_multiblock_sst(&destroyed, &fs)?;
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs.open(
            &destroyed,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(mid_off))?;
        file.write_all(&vec![0u8; mid_size as usize])?;
    }
    assert!(
        !super::dropped_data_extent_is_zeroed(&*fs, &destroyed, &dropped)?,
        "zeros WRITTEN over a block are damage, not reclamation",
    );
    Ok(())
}

/// The punch guard must be STRUCTURE-anchored, not length-anchored: a zero run
/// counts as punch evidence only when it ends where intact structure begins (a
/// decodable block header, the extent end, or the data-section end). SST
/// values are arbitrary bytes, so an ordinary unpunched table whose value
/// carries a header-sized run of zeros inside a surrendered extent must NOT be
/// classified as punched — under the default no-resurrection policy that
/// false positive rejects an otherwise usable salvage as bound-lost.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn salvage_guard_ignores_zero_filled_values_inside_a_surrendered_extent() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    // An UNPUNCHED table whose values embed zero runs longer than a block
    // header, framed mid-payload by non-zero bytes on both sides.
    let mut w =
        crate::table::Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..32u32 {
        let mut value = vec![b'x'; 8];
        value.extend_from_slice(&[0u8; 64]);
        value.extend_from_slice(b"tail");
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            value,
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    assert!(w.finish()?.is_some(), "the SST is non-empty");

    // Self-check the fixture: the raw data region really does contain a zero
    // run at least MIN_RUN long (no compression swallowed it), so a
    // length-anchored guard WOULD have fired here.
    {
        let file = fs.open(&sst, &crate::fs::FsOpenOptions::new().read(true))?;
        let len = crate::fs::FsFile::metadata(&*file)?.len;
        let bytes = crate::file::read_exact(&*file, 0, usize::try_from(len).unwrap_or(0))?;
        let min_run = crate::table::block::Header::MIN_LEN;
        let has_run = bytes.windows(min_run).any(|w| w.iter().all(|&b| b == 0));
        assert!(has_run, "fixture must embed a header-sized zero run");
    }

    // The whole data section surrendered as one extent from its first byte.
    let table = recover_sst(sst.clone(), &fs)?;
    let first_off = {
        use crate::table::block_index::BlockIndex;
        let mut it = table.block_index.iter();
        it.next().expect("at least one block")?.offset().0
    };
    drop(table);
    let dropped = vec![crate::salvage::DroppedBlock {
        offset: first_off,
        section: b"data".to_vec(),
        reason: crate::salvage::DropReason::HeaderCorrupted("surrendered tail".to_owned()),
        key_range: None,
    }];
    assert!(
        !super::dropped_data_extent_is_zeroed(&*fs, &sst, &dropped)?,
        "a zero-filled value inside the surrendered extent is not punch evidence",
    );
    Ok(())
}

/// A repair that commits its manifest and then fails to remove the superseded
/// originals leaves BOTH the originals and the replacements on disk. The retry
/// must not rebuild from all of them: it would salvage the damaged blob again,
/// rewrite the original SST again, and keep the previous rewrite too, so one
/// history would enter L0 twice — and duplicated merge operands are applied
/// twice on read. The committed manifest is the authority on which files are
/// superseded, so the retry finishes that cleanup before scanning.
#[test]
fn repair_retry_after_a_failed_cleanup_does_not_duplicate_the_history() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;

    let config = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    // The manifest commits, then removing the superseded originals fails.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );
    assert!(
        config(Arc::new(fault)).repair().is_err(),
        "the post-commit cleanup failure must propagate",
    );

    // The retry, on a healthy filesystem, finishes that cleanup and rebuilds
    // from what the committed manifest actually names.
    let report = config(memfs.clone()).repair()?;
    assert_eq!(
        report.recovered, 1,
        "one table, not the original beside its own rewrite: {report:?}",
    );

    let tree = match config(memfs).open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    assert_eq!(
        tree.index.current_version().iter_tables().count(),
        1,
        "the rebuilt manifest holds one copy of the history",
    );
    Ok(())
}

/// L0 order decides which run answers first, so it must be derived from the
/// files, not from the order a directory scan happened to yield. Two tables can
/// carry the same highest sequence number — callers may reuse an explicit seqno
/// across separate flushed batches — and sorting by that alone leaves their
/// relative order to hash-map iteration. Ids are allocated in increasing order,
/// so the later table (higher id) is the newer one and belongs nearer the head.
#[test]
fn repair_orders_equal_seqno_tables_by_descending_id() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;

    // Six tables, all at the same highest seqno: enough that an arbitrary
    // iteration order is vanishingly unlikely to match id-descending by chance.
    for id in 0..6u64 {
        use crate::{InternalValue, ValueType};
        let path = tables.join(id.to_string());
        let mut w = crate::table::Writer::new(path, id, 0, Arc::clone(&fs))?;
        w.write(InternalValue::from_components(
            format!("k{id:04}").into_bytes(),
            b"v",
            7,
            ValueType::Value,
        ))?;
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs);
    config.repair()?;

    let tree = config.open()?;
    let version = tree.current_version();
    let ids: Vec<_> = version.iter_tables().map(crate::Table::id).collect();
    assert_eq!(
        ids,
        vec![5, 4, 3, 2, 1, 0],
        "equal-seqno tables must be ordered newest-first by id, not by scan order",
    );
    Ok(())
}

/// A table's highest seqno is not a recency signal at all: callers may assign
/// seqnos explicitly, so an older table can top out above a newer one on an
/// unrelated key. Ordering L0 by it then puts the older table nearer the head
/// and repair serves the superseded value for every key the two share.
#[test]
fn repair_orders_l0_by_table_recency_not_by_maximum_seqno() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fs, MemFs};
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;

    // The OLDER table (id 0) carries a high maximum seqno from an unrelated
    // key; the NEWER one (id 1) tops out lower. Both hold `k` at seqno 10 with
    // different values — the newer table's value is what the tree must serve.
    for (id, other_seqno, value) in [(0u64, 100u64, "stale"), (1, 50, "fresh")] {
        let mut w = crate::table::Writer::new(tables.join(id.to_string()), id, 0, Arc::clone(&fs))?;
        w.write(InternalValue::from_components(
            b"k",
            value.as_bytes(),
            10,
            ValueType::Value,
        ))?;
        // Keys ascend within an SST, so the unrelated key sorts after `k`.
        w.write(InternalValue::from_components(
            b"unrelated",
            b"v",
            other_seqno,
            ValueType::Value,
        ))?;
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs);
    config.repair()?;

    let tree = config.open()?;
    assert_eq!(
        tree.get(b"k", crate::SeqNo::MAX)?.as_deref(),
        Some(b"fresh".as_slice()),
        "the later table's value must win: a maximum seqno drawn from an \
         unrelated key says nothing about which table is newer",
    );
    Ok(())
}

/// Delegates to `MemFs` but hands directory entries back in a FIXED order
/// (ascending by name, or descending), so a test can pin a scan's input order
/// instead of depending on the map iteration a backend happens to produce —
/// `MemFs` hashes its paths, so its own order varies between processes.
struct SortedDirFs(crate::fs::MemFs, bool);

impl crate::fs::Fs for SortedDirFs {
    fn open(
        &self,
        path: &std::path::Path,
        options: &crate::fs::FsOpenOptions,
    ) -> crate::io::Result<Box<dyn crate::fs::FsFile>> {
        self.0.open(path, options)
    }
    fn remove_file(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.0.remove_file(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> crate::io::Result<()> {
        self.0.rename(from, to)
    }
    fn create_dir_all(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.0.create_dir_all(path)
    }
    fn remove_dir_all(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.0.remove_dir_all(path)
    }
    fn sync_directory(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.0.sync_directory(path)
    }
    /// The whole point: the scan sees the entries in a pinned order.
    fn read_dir(&self, path: &std::path::Path) -> crate::io::Result<Vec<crate::fs::FsDirEntry>> {
        let mut entries = self.0.read_dir(path)?;
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        if self.1 {
            entries.reverse();
        }
        Ok(entries)
    }
    fn metadata(&self, path: &std::path::Path) -> crate::io::Result<crate::fs::FsMetadata> {
        self.0.metadata(path)
    }
    fn exists(&self, path: &std::path::Path) -> crate::io::Result<bool> {
        self.0.exists(path)
    }
    fn capabilities(&self, path: &std::path::Path) -> crate::fs::FsCapabilities {
        self.0.capabilities(path)
    }
    fn punch_hole(&self, path: &std::path::Path, offset: u64, len: u64) -> crate::io::Result<()> {
        self.0.punch_hole(path, offset, len)
    }
    fn allocated_size(&self, path: &std::path::Path) -> crate::io::Result<Option<u64>> {
        self.0.allocated_size(path)
    }
    fn extent_is_hole(
        &self,
        path: &std::path::Path,
        offset: u64,
        len: u64,
    ) -> crate::io::Result<Option<bool>> {
        self.0.extent_is_hole(path, offset, len)
    }
}

/// Two spellings of one id (`1` and `01`) can hold different content, so which
/// one survives decides what the rebuilt tree contains. Directory iteration
/// order must not be that decision: the writer's own `{id}` spelling is the
/// canonical file and always wins, as the blob-file scan already guarantees.
/// Both iteration orders are exercised, since a backend's is arbitrary.
#[test]
fn repair_prefers_the_canonical_spelling_of_a_duplicated_id() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    for reversed in [false, true] {
        let fs: Arc<dyn Fs> = Arc::new(SortedDirFs(MemFs::new(), reversed));
        let root = std::path::absolute("/db")?;
        let tables = root.join("tables");
        fs.create_dir_all(&tables)?;

        // Both files carry table id 1 and are structurally complete; only their
        // contents (and names) differ.
        for (name, key) in [("1", "canonical"), ("01", "alternate")] {
            use crate::{InternalValue, ValueType};
            let mut w = crate::table::Writer::new(tables.join(name), 1, 0, Arc::clone(&fs))?;
            w.write(InternalValue::from_components(
                key.as_bytes(),
                b"v",
                5,
                ValueType::Value,
            ))?;
            assert!(w.finish()?.is_some(), "the SST is non-empty");
        }

        let config = Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs));
        config.repair()?;

        let tree = config.open()?;
        assert!(
            tree.get(b"canonical", crate::SeqNo::MAX)?.is_some(),
            "the canonical `{{id}}` spelling must survive (reversed: {reversed})",
        );
        assert!(
            tree.get(b"alternate", crate::SeqNo::MAX)?.is_none(),
            "the alternate spelling must not displace it (reversed: {reversed})",
        );
    }
    Ok(())
}

/// The hole evidence must cover the ZEROED BLOCK, not the file. A file-wide
/// allocation total says nothing about where the missing bytes are — and on a
/// filesystem with transparent compression an ordinary, fully-written file
/// already reports fewer physical bytes than its length, so the file-wide test
/// passes for a table that was never punched at all. Modelled here by a file
/// that is sparse ELSEWHERE while the destroyed block is plain written zeros.
#[test]
fn repair_requires_the_hole_under_the_zeroed_block_itself() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    let first_block_end = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00050")?;
    assert!(first_block_end > 0, "the fixture has a leading block");
    let len =
        crate::fs::FsFile::metadata(&*fs.open(&sst, &crate::fs::FsOpenOptions::new().read(true))?)?
            .len;

    // Destroyed by a WRITE of zeros: no hole under it.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs.open(
            &sst,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&vec![0u8; usize::try_from(first_block_end).unwrap_or(0)])?;
    }
    // The file is nonetheless sparse: a few bytes inside that block are
    // physically unallocated while the block as a whole is not a hole. This
    // stands in for the compressed-filesystem case, where allocation falls below
    // length for reasons that do not make any particular extent a hole.
    memfs.punch_hole(&sst, 0, 8)?;
    assert!(
        fs.allocated_size(&sst)?.is_some_and(|a| a < len),
        "the fixture reports less allocated space than its length",
    );

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs);
    let report = config.repair()?;
    assert_eq!(
        report.recovered, 0,
        "the zeroed block has no hole under it, so it is damage: {report:?}",
    );
    Ok(())
}

/// A fresh id must not be one an orphaned `.restrict-bound` sidecar already
/// names. The scan skips sidecars, so an id whose table is gone still has one
/// lying beside it; publishing an UNRESTRICTED rewrite under that id makes a
/// later manifest-loss repair match the stale sidecar by id and restrict the
/// replacement at an unrelated bound, silently dropping its prefix.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_does_not_publish_a_rewrite_under_an_id_a_sidecar_names() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // A corrupt blob frame forces the referencing table through the handle
    // rewrite, which publishes its copy under a fresh id.
    let blob_path = memfs
        .read_dir(&root.join(crate::file::BLOBS_FOLDER))?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    {
        use std::io::{Seek, SeekFrom, Write};
        let last = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
            .last()
            .expect("a last frame")?;
        let flip_at = last.frame_end - 8;
        let mut file = fs_dyn.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        let byte = crate::file::read_exact(&*file, flip_at, 1)?;
        file.seek(SeekFrom::Start(flip_at))?;
        file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    }

    // An ORPHANED sidecar for table 1 — the id the rewrite would take next.
    let tables = root.join("tables");
    crate::restrict_bound::write(
        &*fs_dyn,
        &tables.join("1"),
        None,
        1,
        b"k0004",
        crate::fs::SyncMode::Normal,
    )?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // The stale sidecar cannot be cleaned up, so the id must not be handed out
    // in the first place — clearing it is best-effort, but the collision is not.
    let fault = crate::fs::FaultFs::new((*memfs).clone());
    fault.injector().arm(
        crate::fs::FaultRule::new(
            crate::fs::FaultOp::RemoveFile,
            crate::fs::Fault::Error(crate::io::ErrorKind::PermissionDenied),
        )
        .on_path("restrict-bound"),
    );
    Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(kv())
    .repair()?;

    // Whatever id the rewrite took, no surviving sidecar may name it: a later
    // repair would restrict that table at a bound belonging to another file.
    let published: Vec<_> = memfs
        .read_dir(&tables)?
        .into_iter()
        .filter(|e| e.file_name.parse::<crate::TableId>().is_ok())
        .map(|e| e.file_name)
        .collect();
    for e in memfs.read_dir(&tables)? {
        let Some(id) = e.file_name.strip_suffix(".restrict-bound") else {
            continue;
        };
        assert!(
            !published.iter().any(|name| name == id),
            "table {id} is published while a stale sidecar names it: a later \
             repair would restrict it at an unrelated bound",
        );
    }
    Ok(())
}

/// Zeros alone are not a reclaim. With no trustworthy sidecar, repair reads a
/// clean zeroed prefix as the mark of a completed punch and restricts the table
/// past it — but corruption that zeroes the leading data block leaves exactly
/// that shape, and then the destroyed block AND the sub-bound rows of the first
/// readable block are dropped while repair reports the table recovered. A punch
/// leaves a physical hole, so the classifier must see one.
#[test]
fn repair_does_not_read_a_zeroed_leading_block_as_a_reclaim() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // Overwrite the first data block with zeros — no hole is punched, so the
    // file stays fully allocated. No sidecar: nothing ever committed a
    // restriction for this table.
    let first_block_end = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00050")?;
    assert!(first_block_end > 0, "the fixture has a leading block");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs.open(
            &sst,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&vec![0u8; usize::try_from(first_block_end).unwrap_or(0)])?;
    }
    assert_eq!(
        fs.allocated_size(&sst)?,
        Some(
            crate::fs::FsFile::metadata(
                &*fs.open(&sst, &crate::fs::FsOpenOptions::new().read(true))?
            )?
            .len
        ),
        "the fixture must be fully allocated: zeros written, not punched",
    );

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs);
    let report = config.repair()?;
    assert_eq!(
        report.recovered, 0,
        "a table whose leading block was DESTROYED must not be published as a \
         restricted view of its own suffix: {report:?}",
    );
    Ok(())
}

/// A committed restriction does not make every zero region a reclaimed one.
/// The prefix punch runs highest-block-first and stops at its first failure, so
/// a failure on the very first call leaves the table with NO hole at all — and
/// then a later live block zeroed by corruption is the first gap the walk
/// meets. Taking that gap as the frontier would skip the destroyed data and
/// report the file clean, so the frontier must come from the table's own index
/// (where the sidecar's bound actually falls), never from the first zeros.
#[test]
fn verify_sst_file_bounds_the_skip_by_the_index_not_by_the_first_zeros() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // A committed restriction low in the key space, and NO punch (the first
    // punch_hole failed). A live block WELL ABOVE the bound is then destroyed.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00010", SyncMode::Normal)?;
    let bound_offset = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00010")?;
    let far_above = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00200")?;
    assert!(
        far_above > bound_offset,
        "the destroyed block sits above the restriction's frontier",
    );
    memfs.punch_hole(&sst, bound_offset, far_above - bound_offset)?;

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        !report.is_ok(),
        "destruction above the restriction frontier must be reported, not \
         mistaken for the reclaimed prefix: errors {:?}, warnings {:?}",
        report.errors,
        report.warnings,
    );
    Ok(())
}

/// A sidecar only attests the restriction of the table it names. Standalone
/// verification has no caller-supplied id, but the SST's own file name carries
/// one — a checksum-valid sidecar recorded for a DIFFERENT id (copied, stale)
/// must not silence a zeroed leading block, or destruction reads as reclaim and
/// the file is pronounced healthy.
#[test]
fn verify_sst_file_rejects_a_sidecar_naming_another_table() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // A sidecar for a DIFFERENT table id beside table 0, and a first data block
    // destroyed by corruption (not a reclaim — nothing committed a restriction
    // for this file).
    crate::restrict_bound::write(&*fs, &sst, None, 7, b"k00050", SyncMode::Normal)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00050")?;
    assert!(punch > 0, "the fixture has a leading block to destroy");
    memfs.punch_hole(&sst, 0, punch)?;

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        !report.is_ok(),
        "a foreign sidecar must not let the zeroed prefix pass as reclaimed: \
         errors {:?}, warnings {:?}",
        report.errors,
        report.warnings,
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
fn verify_sst_file_skips_a_punched_prefix_within_a_read_budget() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs, SyncMode};
    use crate::io::ErrorKind;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    {
        use crate::{InternalValue, ValueType};
        let mut w =
            crate::table::Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
        for i in 0..4096u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                vec![b'v'; 32],
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k04000", SyncMode::Normal)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k04000")?;
    assert!(
        punch > 100_000,
        "the fixture has a sizable reclaimed prefix"
    );
    memfs.punch_hole(&sst, 0, punch)?;

    // The reclaimed prefix must be crossed in bulk reads, not one per byte: a
    // production prefix is gigabytes, where a per-byte walk never finishes.
    // The budget errors the (N+1)-th positioned read, so exceeding it fails the
    // verification loudly instead of hanging the test.
    let budget = FaultFs::new((*memfs).clone());
    budget.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(512),
    );
    let budget_fs: Arc<dyn Fs> = Arc::new(budget);
    let report = crate::verify::verify_sst_file_with_fs(&budget_fs, &sst);
    assert!(
        report.is_ok(),
        "crossing a {punch}-byte reclaimed prefix must stay well inside a \
         512-read budget: errors {:?}, warnings {:?}",
        report.errors,
        report.warnings,
    );
    Ok(())
}

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

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
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
    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        !report.is_ok(),
        "leading zeros without a restriction sidecar are destroyed data and \
         must fail verification: {report:?}",
    );
    Ok(())
}

/// The restricted-verify frontier must be derived by walking FRAMES, not by
/// searching for zero runs. A live value whose bytes end in zeros is followed
/// by the next real block header, which a byte-run scan accepts as a punch
/// boundary — advancing the frontier past an intact block, so the verifier
/// skips it and any corruption inside it while still reporting OK.
#[test]
fn a_zero_tailed_value_does_not_move_the_restricted_verify_frontier() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    // Every value ends in a long zero run — legal payload, and exactly what a
    // byte-run scan mistakes for reclaimed space.
    {
        let mut w =
            crate::table::Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..64u32 {
            let mut value = format!("v{i}").into_bytes();
            value.extend(std::iter::repeat_n(0u8, 64));
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                value,
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // Baseline: no sidecar, so no frontier derivation runs at all.
    let baseline = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        baseline.is_ok(),
        "the unpunched file must verify clean: {baseline:?}",
    );

    // A committed restriction at the very first key. Nothing is punched, so
    // the derived frontier must stay at the data start and the verifier must
    // still walk exactly as many blocks as it did without the sidecar.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00000", SyncMode::Normal)?;
    let restricted = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        restricted.is_ok(),
        "an unpunched file must verify clean under a restriction: {restricted:?}",
    );
    assert_eq!(
        restricted.blocks_scanned, baseline.blocks_scanned,
        "a zero-tailed value must not be mistaken for a punched extent: \
         the frontier would advance past live blocks and the verifier would \
         skip them (and any corruption inside them) while reporting OK",
    );
    Ok(())
}

/// A restriction reclaims a PREFIX, so only leading zeroed extents may move
/// the verify frontier. A live suffix block that was destroyed — zeroed by
/// damage rather than reclaimed — sits after real data, and treating it as
/// another punched extent would start verification past the loss and pronounce
/// the file healthy while skipping exactly the destroyed region.
#[test]
fn a_destroyed_suffix_block_does_not_move_the_restricted_verify_frontier() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    // A committed restriction whose punched prefix is genuine.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00130", SyncMode::Normal)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00130")?;
    memfs.punch_hole(&sst, 0, punch)?;

    // Now zero a block INSIDE the live suffix, as damage would — one with
    // live blocks before it, so it is a genuine interior gap. (A block
    // destroyed immediately at the punch boundary merges into one continuous
    // zero region and is indistinguishable from a larger punch by geometry
    // alone; see the weak-spot note in docs/manifest-recovery.md.)
    let destroyed = recover_sst(sst.clone(), &fs)?
        .data_block_handles()
        .filter_map(Result::ok)
        .map(|keyed| *AsRef::<crate::table::BlockHandle>::as_ref(&keyed))
        .filter(|h| h.offset().0 >= punch)
        .nth(2)
        .ok_or(crate::Error::Unrecoverable)?;
    memfs.punch_hole(&sst, destroyed.offset().0, u64::from(destroyed.size()))?;

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        !report.is_ok(),
        "a destroyed live block must be reported, not skipped as if it were \
         another reclaimed extent: {report:?}",
    );
    Ok(())
}

/// A read that lands in a hole-punched extent must say so. The rows are
/// permanently gone, and the two plausible alternatives are both wrong: a
/// checksum mismatch reads as "the bytes rotted" and invites a heal or scrub
/// that can never succeed, while reporting the key as merely absent would let
/// the lookup fall through to a superseded version in a lower level and
/// silently resurrect it. The zeros identify themselves at read time, so this
/// needs nothing recorded anywhere — which is what lets an in-place excision
/// survive a crash without a journal.
#[test]
fn a_read_into_a_punched_extent_reports_it_as_excised() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;

    let table = recover_sst(sst.clone(), &fs)?;
    let keyed = table
        .data_block_handles()
        .next()
        .transpose()?
        .ok_or(crate::Error::Unrecoverable)?;
    let first: &crate::table::BlockHandle = keyed.as_ref();
    memfs.punch_hole(&sst, first.offset().0, u64::from(first.size()))?;

    let Err(err) = table.load_data_block(first) else {
        panic!("a punched block cannot load");
    };
    assert!(
        matches!(err, crate::Error::Excised { offset } if offset == first.offset().0),
        "a punched extent must be reported as excised, not as damaged bytes: {err:?}",
    );
    Ok(())
}

/// Two same-id copies living in DIFFERENT filesystem namespaces are distinct
/// files even when their paths spell the same string: canonicalizing both
/// through the host filesystem would call them aliases and skip removing
/// the loser, leaving a same-id leftover that a later reopen can resolve
/// against the kept copy's manifest checksum. The alias test must therefore
/// compare backend identity too, and treat "no shared-namespace guarantee"
/// (`backend_id() == None`) as distinct.
#[test]
fn same_physical_file_requires_a_shared_namespace() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, StdFs};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let real = dir.path().join("0");
    std::fs::write(&real, b"real bytes")?;

    let memfs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let stdfs: Arc<dyn Fs> = Arc::new(StdFs);
    // A MemFs file at the very path that also exists on the host filesystem.
    if let Some(parent) = real.parent() {
        memfs.create_dir_all(parent)?;
    }
    {
        use std::io::Write;
        let mut f = memfs.open(
            &real,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(b"virtual bytes")?;
    }

    assert!(
        !super::same_physical_file(&*memfs, &real, &*stdfs, &real),
        "same path spelling in DIFFERENT namespaces is not an alias",
    );
    assert!(
        super::same_physical_file(&*stdfs, &real, &*stdfs, &real),
        "the same host path through one namespace IS an alias",
    );
    Ok(())
}

/// Two DISTINCT virtual files whose path strings happen to canonicalize to
/// one host inode (a host symlink joins the directories) are still distinct:
/// alias resolution belongs to the BACKEND, and a virtual backend's path
/// strings are distinct files by construction. Resolving them through the
/// host would declare the pair aliases, skip removing the duplicate
/// loser, and let a later reopen resolve the leftover against the kept
/// copy's manifest checksum. Unix-only: the fixture needs an unprivileged
/// host symlink.
#[cfg(unix)]
#[test]
fn same_physical_file_ignores_host_symlinks_for_a_virtual_backend() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::Write;
    use std::sync::Arc;

    // Host: a real directory with a file, and a symlink to it, so the two
    // path spellings canonicalize to the SAME host inode.
    let dir = tempfile::tempdir()?;
    let real_dir = dir.path().join("x");
    std::fs::create_dir(&real_dir)?;
    std::fs::write(real_dir.join("0"), b"host bytes")?;
    let linked_dir = dir.path().join("y");
    std::os::unix::fs::symlink(&real_dir, &linked_dir)?;

    // Virtual backend: the SAME two path strings name two DISTINCT files.
    let memfs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let (real_path, linked_path) = (real_dir.join("0"), linked_dir.join("0"));
    for (path, bytes) in [
        (&real_path, b"first".as_slice()),
        (&linked_path, b"second".as_slice()),
    ] {
        if let Some(parent) = path.parent() {
            memfs.create_dir_all(parent)?;
        }
        let mut file = memfs.open(
            path,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        file.write_all(bytes)?;
    }

    assert!(
        !super::same_physical_file(&*memfs, &real_path, &*memfs, &linked_path),
        "a host symlink must not alias two distinct virtual files",
    );
    Ok(())
}

/// A zero run INSIDE a live value must never move the frontier: the derive
/// anchors only on a run whose end is a VALIDATED block header (magic +
/// header checksum), so a value carrying `Header::MIN_LEN` zeros — perfectly
/// legal payload — cannot make the walk start mid-frame and condemn (or
/// silently skip part of) a healthy SST.
#[test]
fn verify_sst_file_ignores_zero_runs_inside_live_values() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs, SyncMode};
    use crate::table::Writer;
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");

    // Values are long all-zero byte strings — legal payload that contains far
    // more than `Header::MIN_LEN` consecutive zeros. No punch anywhere.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
        for i in 0..64u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                vec![0u8; 256],
                u64::from(i) + 1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }
    // A sidecar makes the derive eligible: without it the derive short-circuits
    // and the zero runs could not move the frontier anyway.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00010", SyncMode::Normal)?;

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
    assert!(
        report.is_ok(),
        "zero-filled VALUES are legal payload and must not move the frontier: \
         errors {:?}, warnings {:?}",
        report.errors,
        report.warnings,
    );
    assert!(
        report.blocks_scanned > 0,
        "every live block must still be walked: {report:?}",
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

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
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

/// A repair that fails before its manifest commits must leave the directory
/// EXACTLY as it found it, so the retry re-derives the same answer from the same
/// bytes. Faulting the replacement's creation proves it: the damaged source is
/// still at its own name, and the retry then salvages it normally.
#[test]
fn a_failed_repair_leaves_the_source_where_the_retry_finds_it() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    // Flip a payload byte of the first data block: the container, index and meta
    // stay intact, so recovery opens the table and only the block walk sees the
    // damage — the shape that routes the table through salvage.
    let offset = {
        use crate::table::block_index::BlockIndex;
        let table = recover_sst(sst.clone(), &fs)?;
        let Some(handle) = table.block_index.iter().next().transpose()? else {
            panic!("the fixture has data blocks");
        };
        handle.offset().0
    };
    {
        use crate::fs::FsOpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut bytes = Vec::new();
        fs.open(&sst, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut bytes)?;
        let flip = usize::try_from(offset).unwrap_or(0) + 16;
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
        let mut f = fs.open(&sst, &FsOpenOptions::new().write(true))?;
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }

    // Fault the replacement's creation: the run aborts with the source untouched.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Interrupted))
            .on_path(super::REPAIR_TMP_SUFFIX),
    );
    let failed = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true);
    assert!(
        failed.is_err(),
        "the faulted replacement write must surface"
    );
    assert!(
        fs.exists(&sst)?,
        "the source must still be where the retry scans for it",
    );

    // The retry (no fault) salvages it from those same bytes.
    let retry = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_salvage(true)?;
    assert_eq!(retry.recovered, 1, "{retry:?}");
    assert_eq!(retry.salvaged, 1, "{retry:?}");
    Ok(())
}

/// The pre-salvage first-bytes guard's drop is flag-dependent too: a FULLY
/// punched, sidecar-less SST that fails whole-file recovery is dropped by the
/// default repair and kept by a resurrection repair reading the same bytes.
#[test]
fn resurrection_keeps_a_fully_punched_table_the_default_repair_drops() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    // Default repair: the fully punched source has no bound, so it is dropped
    // and its file removed.
    let dropped_fs = Arc::new(MemFs::new());
    let dropped_dyn: Arc<dyn Fs> = dropped_fs.clone();
    let dropped_root = std::path::absolute("/db-dropped")?;
    let dropped_tables = dropped_root.join("tables");
    dropped_dyn.create_dir_all(&dropped_tables)?;
    let dropped_sst = dropped_tables.join("0");
    write_multiblock_sst(&dropped_sst, &dropped_dyn)?;
    let punch = recover_sst(dropped_sst.clone(), &dropped_dyn)?.punch_offset_for(b"k00130")?;
    dropped_fs.punch_hole(&dropped_sst, 0, punch)?;

    let fault = FaultFs::new(dropped_fs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let first = Config::new(
        &dropped_root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");
    assert!(
        !dropped_dyn.exists(&dropped_sst)?,
        "the dropped table's file is removed, not stashed",
    );

    // Resurrection repair on the same shape: the readable region is kept.
    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    let sst = tables.join("0");
    write_multiblock_sst(&sst, &fs)?;
    let punch = recover_sst(sst.clone(), &fs)?.punch_offset_for(b"k00130")?;
    memfs.punch_hole(&sst, 0, punch)?;

    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(second.recovered, 1, "{second:?}");

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

/// The recovery-failure arm's drop is flag-dependent too: a partially punched,
/// sidecar-less SST whose whole-file recovery also failed is dropped by the
/// default repair (and its file removed) and kept by a resurrection repair
/// reading the same shape.
#[test]
fn resurrection_keeps_a_punched_table_the_salvage_arm_would_drop() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::sync::Arc;

    // One-shot read fault fails the whole-file hash → recovery-failure arm →
    // salvage detects the punched dropped extents → dropped (bound lost).
    let dropped_fs = Arc::new(MemFs::new());
    let dropped_dyn: Arc<dyn Fs> = dropped_fs.clone();
    let dropped_root = std::path::absolute("/db-dropped")?;
    let dropped_tables = dropped_root.join("tables");
    dropped_dyn.create_dir_all(&dropped_tables)?;
    build_partially_punched_prefix_sst(&dropped_fs, &dropped_dyn, &dropped_tables)?;

    let fault = FaultFs::new(dropped_fs.as_ref().clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let first = Config::new(
        &dropped_root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .repair_with_salvage(true)?;
    assert_eq!(first.recovered, 0, "{first:?}");
    assert!(
        !dropped_dyn.exists(&dropped_tables.join("0"))?,
        "the dropped table's file is removed, not stashed",
    );

    // Resurrection repair on the same shape keeps the readable region.
    let memfs = Arc::new(MemFs::new());
    let fs: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let tables = root.join("tables");
    fs.create_dir_all(&tables)?;
    build_partially_punched_prefix_sst(&memfs, &fs, &tables)?;

    let second = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(memfs.as_ref().clone())
    .repair_with_resurrection(true, true)?;
    assert_eq!(second.recovered, 1, "{second:?}");

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
/// on the sidecar write and assert the untouched source is what the retry finds.
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
/// remove the half-finished replacement and propagate, never leave the
/// unpunched, sidecar-less copy in place: a later recovery would open THAT
/// unrestricted and resurrect the sub-bound rows. This must hold for a PERSISTENT
/// failure (ENOSPC-class) as well as a transient one, since the retry cannot
/// re-derive the bound from a fresh unpunched output.
#[cfg(feature = "std")]
fn assert_re_restriction_fault_leaves_the_source_alone(
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

    // The distinguishing evidence: no half-finished replacement survives. Left
    // behind, that unpunched sidecar-less copy is what a later run would adopt —
    // unrestricted, resurrecting exactly the rows the restriction hid.
    assert!(
        !memfs.exists(&super::repair_tmp_path(&sst))?,
        "the fault must remove the unrestrictable replacement",
    );
    assert!(
        memfs.exists(&sst)?,
        "the source is untouched at its table path, ready for the retry",
    );
    Ok(())
}

/// A TRANSIENT fault re-imposing the restriction on a salvaged output removes the
/// half-finished replacement and propagates for retry. The retry then recovers
/// the table restricted, with no sub-bound resurrection.
#[test]
fn repair_keeps_the_source_when_re_restriction_faults_transiently() -> crate::Result<()> {
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    assert_re_restriction_fault_leaves_the_source_alone(ErrorKind::Interrupted)?;

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
/// the restriction must ALSO remove the half-finished replacement, not only
/// transient ones. Recovery cannot re-derive the bound from a fresh unpunched
/// copy, so leaving it in place would let a retry install it UNRESTRICTED and
/// resurrect the sub-bound rows.
#[test]
fn repair_keeps_the_source_when_re_restriction_faults_persistently() -> crate::Result<()> {
    use crate::io::ErrorKind;

    assert_re_restriction_fault_leaves_the_source_alone(ErrorKind::Other)
}

/// A PUNCHED SST with no trustworthy bound (missing sidecar) that ALSO fails
/// whole-file recovery must be set aside, NOT block-salvaged into an unrestricted
/// output: recovery leaves no `Table` to derive a geometry bound from, and salvage
/// re-emits the straddling block's sub-bound rows with nothing to restrict them.
/// Fail closed on the ambiguity.
#[test]
fn repair_salvages_a_sidecarless_sst_whose_leading_bytes_were_destroyed() -> crate::Result<()> {
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

    // Zeros WRITTEN over the leading bytes: no hole, so this is ordinary
    // corruption of an unpunched table, and its later blocks are still
    // recoverable.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs.open(
            &sst,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&[0u8; 128])?;
    }

    // Whole-file recovery fails (as in the punched case), routing repair to the
    // salvage arm.
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
        report.recovered, 1,
        "destroyed leading bytes are not a lost punch bound: the readable blocks \
         must still be salvaged rather than the whole table set aside: {report:?}",
    );
    Ok(())
}

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
    // salvage (reading that source) and the punch probe run unfaulted. So
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

/// A swap that fails midway must PROPAGATE. The committed manifest already names
/// the replacement's content, so a source left in place under it is a tree whose
/// next open reads the damaged bytes against the manifest's checksum. Failing
/// here keeps the two outcomes intact: the retry finishes the swap from the same
/// committed manifest.
#[test]
fn commit_repair_tmp_propagates_a_failed_swap() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, SyncMode};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    std::fs::write(&sst, b"damaged")?;
    let tmp = repair_tmp_path(&sst);
    std::fs::write(&tmp, b"replacement")?;

    let fs = FaultFs::new(StdFs);
    fs.injector().arm(FaultRule::new(
        FaultOp::Rename,
        Fault::Error(ErrorKind::Other),
    ));

    assert!(
        commit_repair_tmp(&fs, &tmp, &sst, SyncMode::Normal).is_err(),
        "a failed swap must not be swallowed",
    );
    assert!(
        fs.exists(&tmp)?,
        "the replacement stays where the retry finds it",
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
/// exact bound and drop the whole table.
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
         dropped: {report:?}",
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
/// repaired database unopenable. Repair must remove it like any foreign file.
#[test]
fn repair_removes_a_foreign_healtmp_suffix() -> crate::Result<()> {
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
        "the foreign .healtmp- file is removed from tables/, not left to break \
         the next open",
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
    let decision = verify_keep_decision(&config, &faulting, &sst, &table, false, true);
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
/// routing it through a salvage that is guaranteed to refuse would throw the
/// table away over dead parity.
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
/// not re-emittable) — even a HEALTHY one is therefore DROPPED rather than
/// riding unverified into the rebuilt manifest; recompacting it under a
/// supported scheme is what re-admits it.
#[test]
fn repair_with_salvage_drops_an_unrecognized_ecc_tombstone_sst() -> crate::Result<()> {
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
        reason.contains("excluded") && reason.contains("recompact"),
        "the reason names the exclusion and the recovery path, got: {reason}",
    );
    assert!(
        fs.metadata(&tables.join("0")).is_err(),
        "the dropped table's file must not be left as an orphan for the next open",
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
/// SST salvage cannot re-emit) drops a perfectly healthy table.
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
        "a healthy table whose forged tail the id cross-check rejects is not \
         dropped: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// A tail meta whose table id is CORRECT but whose ECC descriptor alone was
/// forged (checksum restamped) passes the verify probe's id cross-check, so
/// the probe must not stop there: it has to fall back to the intact MID
/// mirror before treating the table as unscanned. Without the fallback a
/// healthy range-tombstone SST is graded degraded-unscanned off the forged
/// descriptor and dropped even though the MID copy carries the valid one.
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
        "a healthy table whose forged tail descriptor the MID fallback \
         overrides is not dropped: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// A FAILED removal must fail the repair. The rebuilt manifest omits the
/// unverifiable table, so a file left behind is an orphan the next open must
/// sweep — and an open that hits the same persistent failure does not open at
/// all. Reporting success over a tree that will not open is the one outcome
/// recovery must never produce, so the error propagates.
#[test]
fn repair_fails_when_a_dropped_table_cannot_be_removed() -> crate::Result<()> {
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

    // An unverifiable range-tombstone SST: the repair drops it.
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

    // Fail the post-commit removal of the dropped table.
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    injector.arm(
        FaultRule::new(FaultOp::RemoveFile, Fault::Error(ErrorKind::Other))
            .on_path("tables")
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
        "a removal the filesystem refuses must fail the repair — the next open \
         would hit the same error sweeping it, got {result:?}",
    );
    assert!(
        fs.metadata(&sst).is_ok(),
        "the file the removal could not delete is still there for the retry",
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
    // The range is reused below to re-checksum after the parity refresh, which
    // only a Page-ECC build compiles — hence the clone, and hence the lint
    // firing only where that reuse is absent.
    #[cfg_attr(
        not(feature = "page_ecc"),
        expect(
            clippy::redundant_clone,
            reason = "the range is reused by the Page-ECC parity refresh below"
        )
    )]
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
/// id and drop a recoverable table.
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
        let table = crate::table::Table::recover(crate::table::RecoverParams::new(
            sst.clone(),
            checksum,
            7,
            Arc::clone(&fs),
            crate::comparator::default_comparator(),
            Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        ))?;
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
        "a recoverable table is not dropped: {:?}",
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

/// `repair_with_salvage` DROPS an SST whose delete-bitmap section is corrupt
/// rather than recovering it: whole-file recovery refuses it (a corrupt bitmap
/// would resurrect deleted rows) and automated salvage fails closed for the same
/// reason — the "all rows live" degradation is an explicit
/// `SalvageOptions::allow_delete_resurrection` opt-in that automated repair
/// never takes. A repair run WITH resurrection is what takes it.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_drops_a_corrupt_delete_bitmap_sst() -> crate::Result<()> {
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
fn repair_with_salvage_drops_a_toc_hidden_range_tombstone_sst() -> crate::Result<()> {
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
    // Pin the specific gate: a whole-file recovery failure would drop the table
    // with the same counts, so require the refusal to name the TOC-concealment
    // check rather than accepting any exclusion path.
    assert!(
        report
            .unreadable_files
            .iter()
            .any(|(_, reason)| reason.contains("may hide deletion metadata")),
        "the refusal must come from the TOC concealment gate: {:?}",
        report.unreadable_files,
    );
    // The original SST is set aside for inspection, not left in `tables/`
    assert!(
        !tables.join("0").exists(),
        "the dropped table's file must not stay in tables/ as an orphan the \
         next open has to sweep: {:?}",
        report.unreadable_files,
    );
    Ok(())
}

/// The resurrection-on counterpart of
/// [`repair_with_salvage_drops_a_toc_hidden_range_tombstone_sst`], pinning
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
fn repair_with_salvage_drops_a_range_tombstone_renamed_to_a_rebuildable_section()
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
    // drop the table with the same counts. This table carried a range tombstone, so
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
        !dir.path().join("tables").join("0").exists(),
        "the relabeled table is dropped and its file removed: {:?}",
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

    let report = crate::verify::verify_sst_file_with_fs(&fs, &sst);
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
/// whose per-KV digest is known stale, instead of dropping the table as
/// corrupt.
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
    // re-emit its range tombstones: it must be dropped, never kept as a merely
    // parity-degraded table.
    assert_eq!(
        report.recovered, 0,
        "a stale-footer table must not be kept as parity-only degradation: {report:?}",
    );
    assert_eq!(
        report.unreadable, 1,
        "the corrupt table is reported and dropped: {report:?}",
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

/// Manifest-loss repair of a tight-space-punched blob file must restore the
/// live-data frontier from the punch geometry: the manifest's
/// `blob_restrictions` record is the frontier's only durable copy, and a repair
/// that rebuilds the blob with frontier `0` (plus a whole-file digest over the
/// zeroed prefix) leaves a later relocation scan starting inside the punched
/// region. The frontier is a byte offset at a frame boundary, so — unlike the
/// SST bound, which is a key and needs its sidecar — the geometry recovers it
/// EXACTLY: the zeroed run from the data-section start ends where a valid
/// frame decodes.
///
/// The second live frame's value is ALL ZEROS: the probe must anchor on frame
/// structure, never on zero runs alone, so a zero-filled payload inside the
/// live suffix cannot move the frontier.
fn write_three_frame_blob(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    path: &std::path::Path,
) -> crate::Result<()> {
    let fs_dyn: std::sync::Arc<dyn crate::fs::Fs> = memfs.clone();
    let mut w = crate::vlog::blob_file::writer::Writer::new(path, 0, 0, &*fs_dyn)?;
    w.write(b"a", 1, &[b'x'; 300])?;
    w.write(b"b", 2, &[b'y'; 300])?;
    w.write(b"c", 3, &[b'z'; 300])?;
    w.finish()?;
    Ok(())
}

fn blob_validation_config(memfs: std::sync::Arc<crate::fs::MemFs>) -> crate::Config {
    crate::Config::new(
        std::path::PathBuf::from("/db"),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
}

/// A blob file whose stored metadata id disagrees with its FILE NAME is a
/// renamed or swapped file, not damaged content: publishing it under the
/// filename's id would resolve existing SST handles into foreign frames
/// (failed reads, or another generation's value when the key matches), and
/// salvaging it would re-emit the foreign records under that id — laundering
/// the swap. Repair must leave such a file out of the manifest and remove it
/// once that manifest is durable.
#[test]
fn blob_recovery_discards_a_file_whose_metadata_id_disagrees() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    for id in 0u64..2 {
        let path = blobs.join(id.to_string());
        let mut w = crate::vlog::blob_file::writer::Writer::new(&path, id, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 200])?;
        w.finish()?;
    }
    // Swap the two files' NAMES: each file's content (and stored metadata id)
    // now belongs to the other name.
    let (zero, one, tmp) = (blobs.join("0"), blobs.join("1"), blobs.join("swap-tmp"));
    memfs.rename(&zero, &tmp)?;
    memfs.rename(&one, &zero)?;
    memfs.rename(&tmp, &one)?;

    let config = blob_validation_config(Arc::clone(&memfs));
    let recovery = super::recover_blob_files(&config)?;
    assert!(
        recovery.files.is_empty(),
        "no swapped file may be published under its filename's id",
    );
    assert_eq!(
        recovery.unreadable.len(),
        2,
        "both swapped files are set aside: {:?}",
        recovery.unreadable,
    );
    assert!(
        recovery
            .unreadable
            .iter()
            .all(|(_, reason)| reason.contains("disagrees")),
        "the reason names the id mismatch: {:?}",
        recovery.unreadable,
    );
    assert_eq!(
        recovery.discard.len(),
        2,
        "both are queued for removal once the manifest is durable: {:?}",
        recovery.discard,
    );
    Ok(())
}

/// Individually checksum-valid blob frames REORDERED on disk must fail frame
/// validation: every blob reader and the relocation merge scanner rely on the
/// sorted-by-internal-key contract, so a blessed out-of-order file corrupts a
/// later relocation's pointer association. Same regression rule the blob
/// salvage walk applies.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_frame_validation_rejects_reordered_frames() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    memfs.create_dir_all(&root)?;
    let path = root.join("0");
    write_three_frame_blob(&memfs, &path)?;

    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    let (a, b) = (
        entries.first().expect("frame a"),
        entries.get(1).expect("frame b"),
    );
    assert_eq!(
        a.frame_end - a.offset,
        b.frame_end - b.offset,
        "equal-length frames swap cleanly",
    );
    // Swap the first two frames byte-for-byte: each frame's checksum is
    // self-contained, so both stay individually valid — only the order breaks.
    let frame = |e: &crate::vlog::blob_file::scanner::ScanEntry| -> crate::Result<Vec<u8>> {
        Ok(crate::file::read_exact(
            &*memfs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?,
            e.offset,
            usize::try_from(e.frame_end - e.offset).expect("frame fits usize"),
        )?
        .to_vec())
    };
    let (bytes_a, bytes_b) = (frame(a)?, frame(b)?);
    {
        let mut f = memfs.open(
            &path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(a.offset))?;
        f.write_all(&bytes_b)?;
        f.write_all(&bytes_a)?;
    }

    let config = blob_validation_config(memfs);
    assert!(
        super::validate_blob_frames(&config, &path, 0, 0)?.is_none(),
        "reordered (individually valid) frames must fail validation",
    );
    Ok(())
}

/// A compressed frame whose checksum was RE-STAMPED over an undecodable
/// payload frames cleanly (the checksum covers only the on-disk bytes), yet
/// every live read of the value fails. Frame validation must decompress each
/// payload before accepting the file — otherwise the rebuilt manifest
/// launders exactly the corruption its digest is supposed to expose.
// Needs a compressor: the whole point is a COMPRESSED payload whose frame
// checksum was restamped, which cannot be built without one.
#[cfg(feature = "lz4")]
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_frame_validation_rejects_a_restamped_compressed_payload() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    memfs.create_dir_all(&root)?;
    let path = root.join("0");
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    {
        let mut w = crate::vlog::blob_file::writer::Writer::new(&path, 0, 0, &*fs_dyn)?
            .use_compression(crate::CompressionType::Lz4);
        w.write(b"a", 1, b"compressible compressible compressible")?;
        w.finish()?;
    }

    // Overwrite the compressed payload with same-length garbage and RE-STAMP
    // the frame checksum (xxh3_128 over key + value + header_crc bytes), so
    // the frame verifies while the payload no longer decompresses.
    let entry = crate::vlog::BlobFileScanner::new(&path, &*fs_dyn, 0)?
        .next()
        .expect("one frame")?;
    let header_len = u64::try_from(crate::vlog::blob_file::writer::BLOB_HEADER_LEN).expect("small");
    let value_start = entry.offset + header_len + 1; // 1-byte key
    let value_len = usize::try_from(entry.frame_end - value_start).expect("fits");
    let crc_bytes = crate::file::read_exact(
        &*memfs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?,
        entry.offset + 38, // header_crc sits last in the 42-byte header
        4,
    )?
    .to_vec();
    let garbage = vec![0xA5u8; value_len];
    let restamped = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(b"a");
        hasher.update(&garbage);
        hasher.update(&crc_bytes);
        hasher.digest128()
    };
    {
        let mut f = memfs.open(
            &path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(entry.offset + 4))?;
        f.write_all(&restamped.to_le_bytes())?;
        f.seek(SeekFrom::Start(value_start))?;
        f.write_all(&garbage)?;
    }
    // The scanner (framing + raw checksum only) must accept the tampered file:
    // the whole point is that framing checks cannot see this shape.
    assert!(
        crate::vlog::BlobFileScanner::new(&path, &*fs_dyn, 0)?
            .next()
            .expect("one frame")
            .is_ok(),
        "fixture: the restamped frame must still pass the raw scan",
    );

    let config = blob_validation_config(memfs);
    assert!(
        super::validate_blob_frames(&config, &path, 0, 0)?.is_none(),
        "a checksum-restamped, undecodable compressed payload must fail validation",
    );
    Ok(())
}

/// A checksum-valid metadata block whose COUNTERS lie must fail frame
/// validation: blob GC's dead-file arithmetic trusts the recorded
/// uncompressed byte total, so an understated value lets `is_dead` reclaim a
/// file whose uncounted frames are still referenced. The scanned frames are
/// the ground truth; the metadata must agree with them.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_frame_validation_rejects_lying_metadata_counters() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    memfs.create_dir_all(&root)?;
    let victim = root.join("0");
    let donor = root.join("donor");

    // Same id, same key lengths, all-equal metadata field WIDTHS — but the
    // donor holds fewer frames, so its (block-checksum-valid) metadata
    // understates the victim's counters once transplanted.
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    write_three_frame_blob(&memfs, &victim)?;
    {
        let mut w = crate::vlog::blob_file::writer::Writer::new(&donor, 0, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 300])?;
        w.write(b"c", 3, &[b'z'; 300])?;
        w.finish()?;
    }
    let section = |path: &std::path::Path| -> crate::Result<(u64, u64)> {
        let mut f = memfs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let meta = reader.toc().section(b"meta").expect("meta section");
        Ok((meta.pos(), meta.len()))
    };
    let (victim_pos, victim_len) = section(&victim)?;
    let (donor_pos, donor_len) = section(&donor)?;
    assert_eq!(
        victim_len, donor_len,
        "equal-width metadata transplants cleanly"
    );
    let donor_meta = crate::file::read_exact(
        &*memfs.open(&donor, &crate::fs::FsOpenOptions::new().read(true))?,
        donor_pos,
        usize::try_from(donor_len).expect("fits"),
    )?
    .to_vec();
    {
        let mut f = memfs.open(
            &victim,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(victim_pos))?;
        f.write_all(&donor_meta)?;
    }

    let config = blob_validation_config(memfs);
    assert!(
        super::validate_blob_frames(&config, &victim, 0, 0)?.is_none(),
        "metadata counters disagreeing with the scanned frames must fail validation",
    );
    Ok(())
}

/// A PUNCHED blob file's metadata describes the whole original file while the
/// scan covers only the live suffix, so exact-equality checks are impossible —
/// but the subset relation still bounds the metadata from BELOW: its item and
/// byte totals must be at least the suffix totals and its key range must
/// contain the scanned suffix. Understated totals are what blob GC's dead-file
/// arithmetic trusts, so blessing them lets `is_dead` reclaim a file whose
/// uncounted frames are still referenced.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_frame_validation_rejects_understated_metadata_on_a_punched_file() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    memfs.create_dir_all(&root)?;
    let victim = root.join("0");
    let donor = root.join("donor");

    // The donor holds ONE frame, so its (block-checksum-valid) metadata
    // understates even the victim's two-frame live SUFFIX once transplanted.
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    write_three_frame_blob(&memfs, &victim)?;
    {
        let mut w = crate::vlog::blob_file::writer::Writer::new(&donor, 0, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 300])?;
        w.finish()?;
    }
    // Scanning resumes at the second frame — the shape a tight-space punch of
    // the first frame leaves behind (no actual hole is needed here; the
    // frontier alone selects the suffix).
    let suffix_start = crate::vlog::BlobFileScanner::new(&victim, &*fs_dyn, 0)?
        .next()
        .expect("first frame")?
        .frame_end;

    let config = blob_validation_config(Arc::clone(&memfs));
    assert!(
        super::validate_blob_frames(&config, &victim, 0, suffix_start)?.is_some(),
        "fixture: the untampered punched file must pass validation",
    );

    let section = |path: &std::path::Path| -> crate::Result<(u64, u64)> {
        let mut f = memfs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let meta = reader.toc().section(b"meta").expect("meta section");
        Ok((meta.pos(), meta.len()))
    };
    let (victim_pos, victim_len) = section(&victim)?;
    let (donor_pos, donor_len) = section(&donor)?;
    assert_eq!(
        victim_len, donor_len,
        "equal-width metadata transplants cleanly"
    );
    let donor_meta = crate::file::read_exact(
        &*memfs.open(&donor, &crate::fs::FsOpenOptions::new().read(true))?,
        donor_pos,
        usize::try_from(donor_len).expect("fits"),
    )?
    .to_vec();
    {
        let mut f = memfs.open(
            &victim,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(victim_pos))?;
        f.write_all(&donor_meta)?;
    }

    assert!(
        super::validate_blob_frames(&config, &victim, 0, suffix_start)?.is_none(),
        "metadata understating the live suffix must fail validation on a punched file",
    );
    Ok(())
}

/// The RANGE half of the punched-file lower-bound check, isolated from the
/// counter half: the donor's metadata counts exactly as many items and bytes
/// as the victim's live suffix, but its key range ends BELOW the suffix's
/// last key — so only the containment requirement can reject the transplant.
/// A key range that fails to contain the live suffix mislocates the file in
/// every range-based pruning decision built on the rebuilt manifest.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_frame_validation_rejects_a_range_not_containing_the_punched_suffix() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    memfs.create_dir_all(&root)?;
    let victim = root.join("0");
    let donor = root.join("donor");

    // Two frames of the SAME sizes as the victim's live suffix (`b`, `c` at
    // 300 bytes each), so item and byte totals match exactly — but the
    // donor's max key `b` sits below the suffix's last key `c`.
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    write_three_frame_blob(&memfs, &victim)?;
    {
        let mut w = crate::vlog::blob_file::writer::Writer::new(&donor, 0, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 300])?;
        w.write(b"b", 2, &[b'y'; 300])?;
        w.finish()?;
    }
    let suffix_start = crate::vlog::BlobFileScanner::new(&victim, &*fs_dyn, 0)?
        .next()
        .expect("first frame")?
        .frame_end;

    let section = |path: &std::path::Path| -> crate::Result<(u64, u64)> {
        let mut f = memfs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let meta = reader.toc().section(b"meta").expect("meta section");
        Ok((meta.pos(), meta.len()))
    };
    let (victim_pos, victim_len) = section(&victim)?;
    let (donor_pos, donor_len) = section(&donor)?;
    assert_eq!(
        victim_len, donor_len,
        "equal-width metadata transplants cleanly"
    );
    let donor_meta = crate::file::read_exact(
        &*memfs.open(&donor, &crate::fs::FsOpenOptions::new().read(true))?,
        donor_pos,
        usize::try_from(donor_len).expect("fits"),
    )?
    .to_vec();
    {
        let mut f = memfs.open(
            &victim,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(victim_pos))?;
        f.write_all(&donor_meta)?;
    }

    let config = blob_validation_config(memfs);
    assert!(
        super::validate_blob_frames(&config, &victim, 0, suffix_start)?.is_none(),
        "a key range not containing the scanned suffix must fail validation \
         even when the item and byte totals satisfy the lower bounds",
    );
    Ok(())
}

#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_recovery_derives_the_frontier_of_a_punched_blob_file() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    let punched_path = blobs.join("0");
    let whole_path = blobs.join("1");

    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    for (id, path) in [(0u64, &punched_path), (1u64, &whole_path)] {
        let mut w = crate::vlog::blob_file::writer::Writer::new(path, id, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 300])?;
        w.write(b"b", 2, &vec![0u8; 400])?; // zero payload in the live suffix
        w.write(b"c", 3, &[b'y'; 300])?;
        w.finish()?;
    }

    // Frontier = the first frame's end boundary (what a tight-space slice
    // records after relocating frame 0), data start from the SFA TOC.
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&punched_path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 3, "three frames written");
    let frontier = entries.first().expect("first frame").frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&punched_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };

    // The tight-space punch: the consumed prefix reads as zeros.
    memfs.punch_hole(&punched_path, data_start, frontier - data_start)?;

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs);

    let recovery = super::recover_blob_files(&config)?;
    let (files, unreadable) = (recovery.files, recovery.unreadable);
    assert!(
        unreadable.is_empty(),
        "both blob files recover: {unreadable:?}"
    );
    assert_eq!(files.len(), 2, "both blob files recovered");
    let punched = files.iter().find(|f| f.id() == 0).expect("punched file");
    let whole = files.iter().find(|f| f.id() == 1).expect("whole file");

    assert_eq!(
        punched.live_data_start(),
        frontier,
        "repair must derive the punched file's frontier from its geometry"
    );
    let suffix_digest = crate::Checksum::from_raw(super::compute_table_checksum_from(
        &*config.fs,
        &punched_path,
        frontier,
    )?);
    assert_eq!(
        punched.checksum(),
        suffix_digest,
        "the recorded digest must cover the live suffix, not the zeroed prefix"
    );

    assert_eq!(
        whole.live_data_start(),
        0,
        "an unpunched file keeps the whole-file frontier"
    );
    Ok(())
}

/// A PARTIALLY punched blob file recovers with its whole-file metadata, so
/// the rebuilt manifest's garbage accounting must be SEEDED with the punched
/// prefix: those frames can never be observed by a future compaction, so
/// with an empty fragmentation map the recorded stale bytes stay below the
/// metadata totals forever and `is_dead` can never retire the file — even
/// after every surviving suffix handle is dropped.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_seeds_garbage_accounting_for_a_punched_blob_prefix() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fs, MemFs};
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;

    let open_config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };
    {
        let tree = match open_config().open()? {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(
                format!("k{i:04}").as_bytes(),
                alloc::vec![b'a' + u8::try_from(i).expect("small i"); 64],
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }

    // Punch the first TWO frames — the shape a tight-space relocation leaves
    // after consuming a prefix — and record the prefix's expected garbage.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = blobs.join("0");
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 8, "eight separated values");
    let prefix: Vec<_> = entries.iter().take(2).collect();
    let frontier = prefix.last().expect("two frames").frame_end;
    let prefix_bytes: u64 = prefix.iter().map(|e| u64::from(e.uncompressed_len)).sum();
    let prefix_on_disk: u64 = prefix.iter().map(|e| e.value.len() as u64).sum();
    let data_start = {
        let mut f = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    open_config().repair()?;

    let tree = match open_config().open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    let binding = tree.index.version_history.read().latest_version();
    let entry = binding.version.gc_stats().get(&0).copied();
    let entry = entry.expect(
        "the rebuilt manifest must seed garbage accounting for the punched prefix, \
         or the file can never be retired by blob GC",
    );
    assert_eq!(entry.bytes, prefix_bytes, "stale uncompressed bytes");
    assert_eq!(entry.on_disk_bytes, prefix_on_disk, "stale on-disk bytes");
    assert_eq!(entry.len, 2, "two punched-away records");
    Ok(())
}

/// A salvage temp left behind by THIS repair's own failed blob salvage (the
/// salvage recovered nothing and the temp's removal fails PERSISTENTLY) must
/// fail the repair, not be shrugged off: the rebuilt manifest never
/// references it, so the next open classifies it as an orphan and its sweep
/// hits the same removal failure — reporting success for a tree that cannot
/// open would be a lie.
#[test]
fn repair_fails_when_its_own_salvage_temp_cannot_be_removed() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        tree.insert(b"k0", alloc::vec![b'x'; 64], 0);
        tree.flush_active_memtable(0)?;
    }

    // Wreck the ENTIRE data section with non-zero garbage: validation fails
    // (no punch geometry — the bytes are not zeros) and the salvage recovers
    // NOTHING, entering the nothing-recoverable cleanup path.
    let blob_path = root.join(crate::file::BLOBS_FOLDER).join("0");
    let (data_start, data_len) = {
        let mut f = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let data = reader
            .toc()
            .section(b"data")
            .ok_or(crate::Error::InvalidHeader("BlobFile"))?;
        (data.pos(), data.len())
    };
    {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "test fixture data section is tiny"
        )]
        let garbage = alloc::vec![0xA5u8; data_len as usize];
        let mut f = memfs.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        f.seek(SeekFrom::Start(data_start))?;
        f.write_all(&garbage)?;
    }
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // Every removal of the salvage temp fails persistently — including the
    // salvage's own internal discard — so the temp survives to the cleanup.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("salvage-tmp"),
    );
    let fault_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));

    let result = fault_config.repair();
    assert!(
        result.is_err(),
        "repair must FAIL, not report success for a tree that cannot open: {result:?}",
    );

    // Once the filesystem is fixed (no fault), a retry completes the cleanup
    // and produces an openable tree.
    let retry_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    retry_config.repair()?;
    assert!(
        !memfs.exists(&root.join(crate::file::BLOBS_FOLDER).join("0.salvage-tmp"))?,
        "the retry removes the temp",
    );
    retry_config.open()?;
    Ok(())
}

/// A crashed repair's leftover `{id}.salvage-tmp` whose removal fails
/// PERSISTENTLY must fail the repair: the rebuilt manifest never references
/// it, so the next open classifies it as an orphan and its sweep hits the
/// same removal failure — reporting success for a tree that cannot open
/// would be a lie. Quarantine is not an out (the temp is discardable
/// garbage, not damaged data). A retry after the filesystem is fixed
/// completes the sweep.
#[test]
fn repair_fails_when_a_leftover_salvage_temp_cannot_be_removed() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::Write;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        tree.insert(b"k0", alloc::vec![b'x'; 64], 0);
        tree.flush_active_memtable(0)?;
    }

    // The crashed earlier repair's in-progress salvage copy.
    let tmp_path = root.join(crate::file::BLOBS_FOLDER).join("0.salvage-tmp");
    {
        let mut f = memfs.open(
            &tmp_path,
            &crate::fs::FsOpenOptions::new().write(true).create_new(true),
        )?;
        f.write_all(b"partial salvage bytes")?;
    }
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // Its removal fails PERSISTENTLY. (The filter has no separator, so it
    // matches Windows paths too.)
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("salvage-tmp"),
    );
    let fault_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));

    let result = fault_config.repair();
    assert!(
        result.is_err(),
        "repair must FAIL, not report success for a tree that cannot open: {result:?}",
    );

    // Once the filesystem is fixed, a retry sweeps the temp and the tree opens.
    let retry_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    let report = retry_config.repair()?;
    assert!(!memfs.exists(&tmp_path)?, "the retry removes the temp");
    assert_eq!(
        report.recovered, 1,
        "the data survives untouched: {report:?}"
    );
    retry_config.open()?;
    Ok(())
}

/// A fully punched blob whose lagged drop hits a PERSISTENT removal failure
/// must fail the repair, not be left in `blobs/` with a shrug: the rebuilt
/// manifest omits the file, so the next open rediscovers it as an orphan and
/// its sweep hits the same removal failure — reporting success for a tree
/// that cannot open would be a lie. Quarantine is not an out (the walk
/// proved the file holds no live data — nothing to preserve). A retry after
/// the filesystem is fixed completes the drop.
#[test]
fn repair_fails_when_a_fully_punched_blobs_drop_cannot_complete() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        tree.insert(b"k0", alloc::vec![b'x'; 64], 0);
        tree.insert(b"k1", alloc::vec![b'y'; 64], 1);
        tree.flush_active_memtable(0)?;
    }

    // Punch the ENTIRE data section: the relocation completed, only the drop
    // lagged the crash.
    let blob_path = root.join(crate::file::BLOBS_FOLDER).join("0");
    let (data_start, data_end) = {
        let mut f = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let data = reader
            .toc()
            .section(b"data")
            .ok_or(crate::Error::InvalidHeader("BlobFile"))?;
        (data.pos(), data.pos() + data.len())
    };
    memfs.punch_hole(&blob_path, data_start, data_end - data_start)?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // Its removal fails PERSISTENTLY. The path filter avoids a separator so
    // it also matches Windows paths (`...\blobs\0`); the only file removed
    // under `blobs` in this fixture is the consumed blob itself.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );
    let fault_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));

    let result = fault_config.repair();
    assert!(
        result.is_err(),
        "repair must FAIL, not report success for a tree that cannot open: {result:?}",
    );
    assert!(
        memfs.exists(&blob_path)?,
        "nothing is moved or discarded on the failure path",
    );

    // Once the filesystem is fixed, a retry completes the drop and opens.
    let retry_config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    retry_config.repair()?;
    assert!(
        !memfs.exists(&blob_path)?,
        "the retry completes the lagged drop",
    );
    retry_config.open()?;
    Ok(())
}

/// A blob file whose punch consumed EVERY frame is a completed tight-space
/// relocation whose file removal lagged the crash: the frontier walk proves
/// the whole data section reads as zeros, so no live data remains. Repair
/// must finish that interrupted drop — publishing an empty-suffix handle
/// with whole-file metadata instead would leave a file blob GC's stale-byte
/// arithmetic can never retire (its frames are already gone, so the stale
/// count never reaches the recorded totals): an immortal empty file.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_recovery_completes_the_drop_of_a_fully_punched_blob_file() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    let consumed_path = blobs.join("0");
    let live_path = blobs.join("1");

    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    for (id, path) in [(0u64, &consumed_path), (1u64, &live_path)] {
        let mut w = crate::vlog::blob_file::writer::Writer::new(path, id, 0, &*fs_dyn)?;
        w.write(b"a", 1, &[b'x'; 300])?;
        w.write(b"b", 2, &[b'y'; 300])?;
        w.finish()?;
    }

    // Punch the ENTIRE data section: every frame is consumed, only the
    // trailer sections (meta, TOC) survive.
    let (data_start, data_end) = {
        let mut file = fs_dyn.open(&consumed_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let data = reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section");
        (data.pos(), data.pos() + data.len())
    };
    memfs.punch_hole(&consumed_path, data_start, data_end - data_start)?;

    let config = blob_validation_config(Arc::clone(&memfs));
    let recovery = super::recover_blob_files(&config)?;
    assert!(
        recovery.unreadable.is_empty(),
        "a fully consumed file is a completed relocation, not damage: {:?}",
        recovery.unreadable,
    );
    assert_eq!(
        recovery.files.len(),
        1,
        "only the live blob file is published"
    );
    assert_eq!(
        recovery.files.first().map(crate::vlog::BlobFile::id),
        Some(1)
    );
    assert!(
        !memfs.exists(&consumed_path)?,
        "the fully punched file's lagged drop is completed",
    );
    Ok(())
}

/// A RESTRICTED table that also needs its blob handles rewritten must be
/// rewritten AND kept restricted, not set aside whole. Setting it aside drops
/// every live suffix row the restriction was protecting; the salvage output
/// path already knows how to re-impose a bound, which is exactly what keeps
/// the rewrite from resurrecting the sub-bound rows.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_rewrites_a_restricted_table_that_references_a_reshaped_blob() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // The blob's first frame is reclaimed (forcing the handle rewrite) and the
    // SST carries a committed restriction bound: the compound state.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    // Reclaim the first THREE frames while the bound hides only the first key:
    // the restricted view still holds handles below the frontier, which is what
    // makes the rewrite necessary on a restricted table.
    let frontier = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .take(3)
        .last()
        .expect("three frames")?
        .frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;

    let source = root.join("tables").join("0");
    crate::restrict_bound::write(
        &*fs_dyn,
        &source,
        None,
        0,
        b"k0001",
        crate::fs::SyncMode::Normal,
    )?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(kv());
    let report = config.repair()?;
    assert_eq!(
        report.recovered, 1,
        "the restricted table is rewritten, not set aside: {report:?}",
    );

    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for i in 3..8u32 {
        let key = format!("k{i:04}");
        assert!(
            tree.get(key.as_bytes(), crate::SeqNo::MAX)?.is_some(),
            "{key} is in the live suffix with an intact record and must survive",
        );
    }
    assert!(
        tree.get(b"k0000", crate::SeqNo::MAX)?.is_none(),
        "the key below the restriction bound must stay hidden",
    );
    Ok(())
}

/// The post-commit swap of a rewritten source must use THAT source's backend. A
/// per-level route stores its tables in a namespace the primary filesystem
/// cannot see, so resolving the path through the primary one either fails (after
/// the manifest has already committed, leaving the routed table unpublished) or
/// touches a same-named file in the wrong namespace.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_swaps_a_routed_rewrite_through_its_own_backend() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let hotfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let hot = std::path::absolute("/hot")?;
    let config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .level_routes(vec![LevelRoute {
            levels: 0..2,
            path: hot.clone(),
            fs: hotfs.clone(),
        }])
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    {
        let tree = match config().open()? {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    let routed_source = hot.join("tables").join("0");
    assert!(
        hotfs.exists(&routed_source)?,
        "the fixture's SST is on the routed tier",
    );

    // A corrupt blob frame salvages the blob, which forces the referencing
    // table through the handle rewrite.
    let blob_path = memfs
        .read_dir(&root.join(crate::file::BLOBS_FOLDER))?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    {
        use std::io::{Seek, SeekFrom, Write};
        let last = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
            .last()
            .expect("a last frame")?;
        let flip_at = last.frame_end - 8;
        let mut file = fs_dyn.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        let byte = crate::file::read_exact(&*file, flip_at, 1)?;
        file.seek(SeekFrom::Start(flip_at))?;
        file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    }
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    config().repair()?;
    assert!(
        hotfs.exists(&routed_source)?,
        "the rewritten table takes its own name on the tier it lives on",
    );
    assert!(
        !hotfs.exists(&super::repair_tmp_path(&routed_source))?,
        "the swap completed on that tier, leaving no unpublished replacement",
    );
    config().open()?;
    Ok(())
}

/// An output whose restriction could not be re-imposed must not stay in
/// `tables/` under a numeric name. It carries the straddling block's sub-bound
/// rows and has no sidecar, so a later repair adopts it UNRESTRICTED and
/// resurrects exactly the rows the restriction hid. When it cannot be removed
/// it must be moved out of the scan's way, and if that fails too the repair
/// fails rather than leaving it discoverable.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_does_not_leave_an_unrestrictable_rewrite_in_the_scan() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // The compound state: a restricted source whose blob is SALVAGED (one
    // corrupt frame), so every surviving record — including the sub-bound rows
    // of the straddling block — is re-emitted through the handle rewrite.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    {
        use std::io::{Seek, SeekFrom, Write};
        let last = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
            .last()
            .expect("a last frame")?;
        let flip_at = last.frame_end - 8; // inside the last frame's payload
        let mut file = fs_dyn.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        let byte = crate::file::read_exact(&*file, flip_at, 1)?;
        file.seek(SeekFrom::Start(flip_at))?;
        file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    }
    let source = root.join("tables").join("0");
    crate::restrict_bound::write(
        &*fs_dyn,
        &source,
        None,
        0,
        b"k0001",
        crate::fs::SyncMode::Normal,
    )?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // The copy's own sidecar cannot be written (so it would be adopted
    // unrestricted) and it cannot be removed either.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("restrict-bound"),
    );
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("tables"),
    );
    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(kv())
    .repair();
    assert!(
        result.is_err(),
        "the failed restriction propagates: {result:?}"
    );

    // Whatever happened, the next scan must not find an unrestricted copy of
    // the restricted table.
    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(kv());
    config.repair()?;
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    assert!(
        tree.get(b"k0000", crate::SeqNo::MAX)?.is_none(),
        "the row below the restriction bound must stay hidden: an unrestrictable \
         rewrite left in tables/ resurrects it",
    );
    Ok(())
}

/// A blob-handle rewrite must keep its SOURCE discoverable until the rebuilt
/// manifest names the copy. The copy is built under a name no scan adopts and
/// swapped onto the source's name only after the commit: displacing the source
/// first leaves a window in which a crash leaves no readable copy where the
/// retry looks, and those rows vanish from the rebuilt manifest.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_keeps_the_rewrite_source_in_place_until_the_manifest_is_committed() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Punch the blob's first frame: the file stays valid, but the SST's stale
    // handle into the reclaimed prefix forces the handle-rewrite path.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    let frontier = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .next()
        .expect("a first frame")?
        .frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }
    let source = root.join("tables").join("0");
    assert!(memfs.exists(&source)?, "the fixture's SST is at tables/0");

    // Fail the manifest commit (the CURRENT pointer's atomic swap): everything
    // up to publication has run.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::PermissionDenied))
            .on_path(crate::file::CURRENT_VERSION_FILE),
    );
    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(kv())
    .repair();
    assert!(result.is_err(), "the commit fails: {result:?}");
    assert!(
        memfs.exists(&source)?,
        "the source must still be in tables/ when the commit fails: a retry \
         scans tables/, so a source moved out early is a table whose keys \
         silently vanish from the rebuilt manifest",
    );

    // The retry (filesystem healthy) rebuilds from that source and every key
    // is readable through the rewritten handles.
    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(kv());
    config.repair()?;
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    // Every key except the one whose value lived in the punched frame (that
    // record is genuinely gone; the rewrite drops entries pointing below the
    // frontier).
    for i in 1..8u32 {
        let key = format!("k{i:04}");
        assert!(
            tree.get(key.as_bytes(), crate::SeqNo::MAX)?.is_some(),
            "{key} survives the rewrite",
        );
    }
    Ok(())
}

/// A blob file no recovered table references is left out of the rebuilt
/// manifest — so repair must also REMOVE it, and fail if it cannot. Left in
/// `blobs/`, it is an orphan the next open sweeps, and if that sweep hits the
/// same removal failure the open fails while repair reported success.
#[test]
fn repair_counts_only_blob_files_that_reach_the_manifest() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fs, MemFs};
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));
    let progress = Arc::new(crate::RecoveryProgress::default());

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        tree.insert(b"k0", alloc::vec![b'x'; 64], 0);
        tree.flush_active_memtable(0)?;
    }

    // An intact blob file no SST points at, written under its OWN id (a byte
    // copy would carry the original's id in its metadata and be rejected as a
    // mismatch, never reaching the counter at all).
    let orphan = root.join(crate::file::BLOBS_FOLDER).join("1");
    let mut w = crate::vlog::blob_file::writer::Writer::new(&orphan, 1, 0, &*fs_dyn)?;
    w.write(b"z", 9, &[b'z'; 300])?;
    w.finish()?;

    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(kv())
    .with_recovery_progress(Arc::clone(&progress))
    .repair()?;

    let snap = progress.snapshot();
    assert_eq!(
        snap.blob_files_discovered, 2,
        "both files are seen by the scan: {snap:?}",
    );
    assert_eq!(
        snap.blob_files_recovered, 1,
        "only the referenced file reaches the manifest, and the counter must \
         say so rather than claim a recovery the reference filter undid: {snap:?}",
    );
    Ok(())
}

#[test]
fn repair_fails_when_an_unreferenced_blob_cannot_be_removed() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let kv = || Some(KvSeparationOptions::default().separation_threshold(16));

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(kv())
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        tree.insert(b"k0", alloc::vec![b'x'; 64], 0);
        tree.flush_active_memtable(0)?;
    }

    // A second, intact blob file no SST points at: what a crashed relocation
    // (or a crashed earlier repair's salvage replacement) leaves behind.
    let orphan = root.join(crate::file::BLOBS_FOLDER).join("1");
    let mut w = crate::vlog::blob_file::writer::Writer::new(&orphan, 1, 0, &*fs_dyn)?;
    w.write(b"z", 9, &[b'z'; 300])?;
    w.finish()?;

    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // Its removal fails PERSISTENTLY. The path filter carries no separator so
    // it matches Windows paths too.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );
    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(kv())
    .repair();
    assert!(
        result.is_err(),
        "repair must FAIL, not report success for a tree whose next open must \
         sweep a file it cannot remove: {result:?}",
    );

    // Once the filesystem is fixed, a retry removes it and the tree opens.
    let retry = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(kv());
    retry.repair()?;
    assert!(
        !memfs.exists(&orphan)?,
        "the unreferenced blob is removed by the repair that omits it",
    );
    retry.open()?;
    Ok(())
}

/// A zeroed TAIL is only punch geometry when nothing live sits below it.
/// Reclaim punches the consumed prefix top-down from the data start, so zeros
/// that FOLLOW intact, structure-anchored frames cannot be a completed
/// relocation — they are destroyed data. Reading them as "every frame was
/// consumed" would delete a file whose live frames are still referenced, and
/// the dependency filter would then set aside every table pointing at it.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn blob_recovery_keeps_a_file_whose_zeroed_tail_follows_live_frames() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    let path = blobs.join("0");

    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let mut w = crate::vlog::blob_file::writer::Writer::new(&path, 0, 0, &*fs_dyn)?;
    w.write(b"a", 1, &[b'x'; 300])?;
    w.write(b"b", 2, &[b'y'; 300])?;
    w.write(b"c", 3, &[b'z'; 300])?;
    w.finish()?;

    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 3, "three frames written");
    let frontier = entries.first().expect("first frame").frame_end;
    let tail_start = entries.get(1).expect("second frame").frame_end;
    let (data_start, data_end) = {
        let mut file = fs_dyn.open(&path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let data = reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section");
        (data.pos(), data.pos() + data.len())
    };

    // A relocated first frame (punched prefix), an intact live second frame,
    // and a destroyed third frame that reads as zeros to the section end.
    memfs.punch_hole(&path, data_start, frontier - data_start)?;
    memfs.punch_hole(&path, tail_start, data_end - tail_start)?;

    let config = blob_validation_config(Arc::clone(&memfs));
    let recovery = super::recover_blob_files(&config)?;
    assert!(
        memfs.exists(&path)?,
        "a zeroed tail below live frames is damage, not a completed relocation: \
         the file must not be dropped",
    );
    // The damaged tail cannot be published as-is, so the intact frame is
    // salvaged into a fresh file — but it MUST survive: reporting the file
    // consumed would have discarded it along with the whole file.
    assert_eq!(
        recovery.files.len(),
        1,
        "the live frame is published: {:?}",
        recovery.unreadable,
    );
    let published = recovery.files.first().expect("a published blob file");
    let keys: Vec<_> = crate::vlog::BlobFileScanner::new(
        blobs.join(published.id().to_string()),
        &*fs_dyn,
        published.id(),
    )?
    .map(|entry| entry.map(|entry| entry.key))
    .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(
        keys,
        vec![crate::UserKey::from(b"b".as_slice())],
        "only the intact frame survives: the relocated one was punched away, \
         the destroyed one is unreadable",
    );
    Ok(())
}

/// A crashed repair can leave its in-progress blob-salvage copy behind. That
/// copy is published by an atomic rename, so a surviving one is never
/// referenced by any manifest — it must be swept, not treated as a foreign
/// file name. The plain open sweeps it as an orphan (a name-parse failure
/// there would leave the tree unopenable); a repair removes it and re-derives
/// the salvage from the original.
#[test]
fn a_crashed_blob_salvage_temp_is_swept_not_fatal() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::Write;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..4u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }
    // Strand a salvage temp the way a crash would.
    let temp = root.join(crate::file::BLOBS_FOLDER).join("0.salvage-tmp");
    {
        let mut f = memfs.open(
            &temp,
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(b"partial salvage output")?;
    }

    // A plain open sweeps it and still serves every key.
    let tree = match Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for i in 0..4u32 {
        assert!(
            tree.get(format!("k{i:04}").as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "record k{i:04} must still read with a stranded salvage temp present",
        );
    }
    drop(tree);
    assert!(
        !memfs.exists(&temp)?,
        "the plain open must sweep the stranded salvage temp",
    );
    Ok(())
}

/// A blob salvage must never leave a HALF-PUBLISHED state, whatever moment a
/// transient fault strikes. The compacted replacement is only usable together
/// with the offset remap this invocation derives, so a retry that found an
/// unverified replacement under the canonical name would bless it as an
/// ordinary intact blob and publish the referencing SSTs with their OLD
/// offsets — handles resolving to the wrong records. The salvage therefore
/// builds into a private temp and publishes atomically: at every fault timing
/// the tree is either fully repaired or exactly as it was found.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_never_half_publishes_a_salvaged_blob_under_transient_faults() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let read_all = |fs: &Arc<dyn Fs>, path: &std::path::Path| -> crate::Result<Vec<u8>> {
        let file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
        let len = crate::fs::FsFile::metadata(&*file)?.len;
        Ok(crate::file::read_exact(&*file, 0, usize::try_from(len).unwrap_or(0))?.to_vec())
    };

    // Builds a fresh tree whose single blob file has a corrupt LAST record and
    // whose manifest is gone. Returns the fs, the blob path, and its bytes.
    let fixture = || -> crate::Result<(Arc<MemFs>, std::path::PathBuf, Vec<u8>)> {
        let memfs = Arc::new(MemFs::new());
        {
            let tree = match Config::new(
                &root,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(memfs.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(16),
            ))
            .open()?
            {
                crate::AnyTree::Blob(t) => t,
                crate::AnyTree::Standard(_) => panic!("expected blob tree"),
            };
            for i in 0..8u32 {
                tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
            }
            tree.flush_active_memtable(0)?;
        }
        let fs_dyn: Arc<dyn Fs> = memfs.clone();
        let blob_path = memfs
            .read_dir(&blobs)?
            .into_iter()
            .find(|e| !e.is_dir)
            .expect("one blob file")
            .path;
        let last = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
            .collect::<crate::Result<Vec<_>>>()?
            .last()
            .expect("a last frame")
            .frame_end;
        {
            let mut file = memfs.open(
                &blob_path,
                &crate::fs::FsOpenOptions::new().read(true).write(true),
            )?;
            let byte = crate::file::read_exact(&*file, last - 8, 1)?;
            file.seek(SeekFrom::Start(last - 8))?;
            file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
        }
        for e in memfs.read_dir(&root)? {
            let is_version = e
                .file_name
                .strip_prefix('v')
                .is_some_and(|rest| rest.parse::<u64>().is_ok());
            if is_version || e.file_name == "current" {
                memfs.remove_file(&e.path)?;
            }
        }
        let corrupted = read_all(&fs_dyn, &blob_path)?;
        Ok((memfs, blob_path, corrupted))
    };

    // Sweep the fault across the whole blob-salvage read sequence, so no single
    // step (frame scan, digest, metadata re-read) escapes the invariant.
    for skip in [0u64, 4, 8, 16, 24, 32, 48, 64, 96, 128] {
        let (memfs, blob_path, corrupted) = fixture()?;
        let fs_dyn: Arc<dyn Fs> = memfs.clone();
        let fault = FaultFs::new((*memfs).clone());
        fault.injector().arm(
            FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted))
                .on_path("blobs")
                .skip(skip),
        );

        let result = Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(fault)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .repair();

        // No salvage temp is ever left behind, whatever the outcome.
        assert!(
            !memfs.exists(&blobs.join("0.salvage-tmp"))?,
            "skip={skip}: a salvage temp must never survive the repair",
        );
        if result.is_err() {
            // Aborted: the canonical path still holds the ORIGINAL bytes, so a
            // retry re-salvages and re-derives the remap.
            assert_eq!(
                read_all(&fs_dyn, &blob_path)?,
                corrupted,
                "skip={skip}: an aborted repair must leave the original blob at \
                 the canonical path, never an unverified replacement",
            );
        } else {
            // Completed: the replacement is published AND the referencing table
            // was rewritten onto its offsets, so every surviving record reads.
            let tree = match Config::new(
                &root,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(memfs.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(16),
            ))
            .open()?
            {
                crate::AnyTree::Blob(t) => t,
                crate::AnyTree::Standard(_) => panic!("expected blob tree"),
            };
            for i in 0..7u32 {
                assert!(
                    tree.get(format!("k{i:04}").as_bytes(), crate::MAX_SEQNO)?
                        .is_some(),
                    "skip={skip}: record k{i:04} must survive a completed repair",
                );
            }
        }
    }
    Ok(())
}

/// A TRANSIENT failure during the blob-handle rewrite must leave the tree
/// exactly as it found it, so the retry rebuilds from the same inputs: every
/// source SST stays in `tables/` (the only directory a repair scans), and the
/// retry recovers its keys. A source moved out of the way before the copy is
/// published would instead be a table whose keys silently vanish.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_leaves_every_table_in_place_when_the_rewrite_fails_transiently() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Punch the blob's first frame: its remaining frames stay VALID (so the blob
    // itself is never salvaged), while the pre-relocation SST's stale handle
    // forces the handle-rewrite path — the only rewrite in this fixture.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    let frontier = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .next()
        .expect("a first frame")?
        .frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    // Fault the rewrite's reads of the source SST with a RETRYABLE kind.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted)).on_path("tables"),
    );

    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .repair();
    assert!(
        result.is_err(),
        "a transient rewrite failure must propagate for a retry: {result:?}",
    );
    assert!(
        memfs.exists(&root.join("tables").join("0"))?,
        "the source SST must still be in tables/ when the retryable error \
         propagates, or the retry rebuilds a manifest without its keys",
    );

    // The retry, on a healthy filesystem, recovers from that source.
    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    config.repair()?;
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for i in 1..8u32 {
        let key = format!("k{i:04}");
        assert!(
            tree.get(key.as_bytes(), crate::SeqNo::MAX)?.is_some(),
            "{key} is recovered by the retry",
        );
    }
    Ok(())
}

/// A blob file with a checksum-corrupt value frame must not be blessed as-is:
/// restamping a digest over the damaged bytes would launder the corruption
/// past every later integrity check while reads of the affected value still
/// fail. Repair instead SALVAGES the blob (a compacted copy under a fresh id
/// holding every intact record) and REWRITES the referencing
/// SSTs through the salvage offset map — surviving records keep working, the
/// lost record's entry is dropped (its key reads as absent, never an error).
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_salvages_a_frame_corrupt_blob_and_remaps_handles() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let value = |i: u32| alloc::vec![b'a' + u8::try_from(i).expect("small i"); 64];

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), value(i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Corrupt the LAST record's payload (the salvage walk keeps everything
    // before the first damaged frame, so only this record is lost).
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 8, "eight separated values");
    let last = entries.last().expect("last frame");
    let flip_at = last.frame_end - 8; // inside the last frame's payload
    {
        let mut file = memfs.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        let byte = crate::file::read_exact(&*file, flip_at, 1)?;
        file.seek(SeekFrom::Start(flip_at))?;
        file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    }
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .repair()?;
    assert_eq!(
        report.recovered, 1,
        "the referencing table survives, rewritten through the remap: {report:?}",
    );
    // The salvaged replacement IS in the rebuilt manifest, so it must be
    // reported as a salvage outcome — never in `unreadable_files`, whose
    // contract is "left out of the manifest".
    assert_eq!(
        report.unreadable, 0,
        "an installed salvaged blob is not an unreadable file: {report:?}",
    );
    assert_eq!(
        report.blob_files_salvaged.len(),
        1,
        "the salvage outcome is reported in its own field: {report:?}",
    );
    assert!(
        report
            .blob_files_salvaged
            .first()
            .is_some_and(|(_, note)| note.contains("records salvaged")),
        "the note describes what was recovered: {report:?}",
    );

    // Reopen: every intact record reads its value; the lost record's key is
    // ABSENT (its entry was dropped), never a read error.
    let tree = match Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for i in 0..7u32 {
        assert_eq!(
            tree.get(format!("k{i:04}").as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(value(i).as_slice()),
            "intact record k{i:04} must survive the blob salvage + handle remap",
        );
    }
    assert_eq!(
        tree.get(b"k0007", crate::MAX_SEQNO)?,
        None,
        "the corrupt record's key reads as absent, never as an error",
    );
    Ok(())
}

/// A repair that fails part-way must leave the tree byte-for-byte as it was
/// found, so the retry re-derives everything from the untouched originals.
/// This is what makes recovery safe without a journal: the salvaged
/// replacement is written under a FRESH blob id and the damaged original is
/// left alone, so nothing a crashed attempt did has to be understood — or
/// undone — by the next one. The fault here lands in the table-rewrite
/// stage, after the blob stage has already produced a replacement.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_failed_repair_leaves_the_originals_intact_and_the_retry_succeeds() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;
    let value = |i: u32| alloc::vec![b'a' + u8::try_from(i).expect("small i"); 64];

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), value(i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = blobs.join("0");
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 8, "eight separated values");

    // A tight-space punch consumed the first two frames; then the LAST
    // frame's payload rots. Validation fails, so the salvage re-emits the
    // surviving middle frames from the replacement's data start — every
    // surviving offset SHIFTS down, making the remap non-identity.
    let frontier = entries.get(1).expect("second frame").frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;
    let last = entries.last().expect("last frame");
    let flip_at = last.frame_end - 8; // inside the last frame's payload
    {
        let mut file = memfs.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        let byte = crate::file::read_exact(&*file, flip_at, 1)?;
        file.seek(SeekFrom::Start(flip_at))?;
        file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    }
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    let config = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    // First attempt: the blob stage produces a replacement, then the table
    // stage hits a transient fault reading its source and propagates it.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted)).on_path("tables"),
    );
    let result = config(Arc::new(fault)).repair();
    assert!(
        result.is_err(),
        "the transient table-stage failure must propagate: {result:?}",
    );
    // The damaged original is still there, untouched: nothing was published
    // over it, so the retry has the same inputs the first attempt had.
    assert!(
        memfs.exists(&blob_path)?,
        "a failed repair must leave the damaged original in place — the \
         tables it has not rewritten yet still reference it",
    );

    // Retry with the fault gone: it re-derives the whole picture from those
    // untouched originals; no state from the crashed attempt is consulted.
    let report = config(memfs.clone()).repair()?;
    assert_eq!(
        report.recovered, 1,
        "the referencing table survives the retry, rewritten: {report:?}",
    );

    let tree = match config(memfs).open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for i in 2..7u32 {
        assert_eq!(
            tree.get(format!("k{i:04}").as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(value(i).as_slice()),
            "surviving record k{i:04} must read through the retry-finished remap",
        );
    }
    for lost in [0u32, 1, 7] {
        assert_eq!(
            tree.get(format!("k{lost:04}").as_bytes(), crate::MAX_SEQNO)?,
            None,
            "punched/corrupt record k{lost:04} reads as absent, never an error",
        );
    }
    Ok(())
}

/// Punches a blob file's first two frames and rots the last frame's payload,
/// so validation fails and the salvage re-emits the surviving middle frames
/// from the replacement's start — a non-identity remap.
#[expect(clippy::expect_used, reason = "test code")]
fn punch_and_corrupt_blob(
    memfs: &crate::fs::MemFs,
    blob_path: &std::path::Path,
) -> crate::Result<()> {
    use crate::fs::Fs;
    use std::io::{Seek, SeekFrom, Write};
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(blob_path, memfs, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 8, "eight separated values");
    let frontier = entries.get(1).expect("second frame").frame_end;
    let data_start = {
        let mut file = memfs.open(blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(blob_path, data_start, frontier - data_start)?;
    let last = entries.last().expect("last frame");
    let flip_at = last.frame_end - 8; // inside the last frame's payload
    let mut file = memfs.open(
        blob_path,
        &crate::fs::FsOpenOptions::new().read(true).write(true),
    )?;
    let byte = crate::file::read_exact(&*file, flip_at, 1)?;
    file.seek(SeekFrom::Start(flip_at))?;
    file.write_all(&[byte.first().expect("one byte") ^ 0xFF])?;
    Ok(())
}

/// The inputs may CHANGE between a failed repair and its retry — here a
/// second blob file is damaged in between — and the retry must still produce
/// a correct tree. It does because it carries nothing forward: each run
/// re-derives its whole picture from the artifacts it finds, so a larger set
/// of damage is simply a different derivation, not a reconciliation problem.
/// (An earlier design recorded what the previous run had done and had to
/// recognise it again afterwards, which is exactly what a changing input set
/// breaks.)
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_retry_handles_damage_that_appeared_after_the_failed_attempt() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let value = |prefix: u8, i: u32| alloc::vec![prefix + u8::try_from(i).expect("small i"); 64];

    // Two flushes → two SSTs, each referencing its OWN blob file (0 and 1).
    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("a{i:04}").as_bytes(), value(b'a', i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        for i in 0..8u32 {
            tree.insert(
                format!("b{i:04}").as_bytes(),
                value(b'b', i),
                u64::from(8 + i),
            );
        }
        tree.flush_active_memtable(0)?;
    }

    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    let config = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    // First attempt: blob 0 is salvaged, then the table stage fails.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted)).on_path("tables"),
    );
    let result = config(Arc::new(fault)).repair();
    assert!(
        result.is_err(),
        "the table-stage failure must propagate: {result:?}",
    );

    // Blob 1 is damaged BETWEEN the attempts, so the retry faces MORE damage
    // than the failed attempt did.
    punch_and_corrupt_blob(&memfs, &blobs.join("1"))?;

    // The retry salvages both blobs and rewrites both tables, deriving all of
    // it from the originals, which the failed attempt left untouched.
    let report = config(memfs.clone()).repair()?;
    assert_eq!(
        report.recovered, 2,
        "both referencing tables survive the retry: {report:?}",
    );

    let tree = match config(memfs).open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    for (prefix, tag) in [(b'a', "a"), (b'b', "b")] {
        for i in 2..7u32 {
            assert_eq!(
                tree.get(format!("{tag}{i:04}").as_bytes(), crate::MAX_SEQNO)?
                    .as_deref(),
                Some(value(prefix, i).as_slice()),
                "surviving record {tag}{i:04} must read after the retry \
                 (a stamped table passed through an applied remap again would \
                 drop its live records)",
            );
        }
        for lost in [0u32, 1, 7] {
            assert_eq!(
                tree.get(format!("{tag}{lost:04}").as_bytes(), crate::MAX_SEQNO)?,
                None,
                "punched/corrupt record {tag}{lost:04} reads as absent, never an error",
            );
        }
    }
    Ok(())
}

/// Zeros are not a reclaim. Corruption that zeroes the leading blob records
/// leaves exactly the shape a completed punch does — a zeroed prefix with a
/// valid frame at its end — so a structure-only classifier promotes it to a
/// frontier, drops every handle below the fabricated bound, and reports the
/// repair a success. A punch DEALLOCATES, so the run must be a proven hole.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_zeroed_blob_prefix_without_a_hole_is_not_a_frontier() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;

    // Overwrite the first two records with zeros WITHOUT punching: the file
    // stays fully allocated, so the zeros are damage, not geometry.
    let frontier =
        crate::vlog::BlobFileScanner::new(&blob_path, &*(memfs.clone() as Arc<dyn Fs>), 0)?
            .nth(1)
            .expect("a second frame")?
            .frame_end;
    let data_start = {
        let mut file = memfs.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    {
        let mut file = memfs.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(data_start))?;
        file.write_all(&vec![
            0u8;
            usize::try_from(frontier - data_start).unwrap_or(0)
        ])?;
        file.sync_all()?;
    }

    let fs: Arc<dyn Fs> = memfs;
    let derived = super::derive_blob_frontier(&fs, &blob_path, 0)?;
    assert!(
        matches!(derived, super::BlobFrontier::Whole),
        "allocated zeros are corruption the validation scan must surface, not a \
         frontier that silently drops the handles below it, got {derived:?}",
    );
    Ok(())
}

/// The same rule at the extreme: a data section zeroed end to end by corruption
/// must not read as a completed relocation, which REMOVES the file (and every
/// table referencing it) while its records were merely damaged.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_wholly_zeroed_blob_section_without_holes_is_not_fully_consumed() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;

    let (data_start, data_len) = {
        let mut file = memfs.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let data = reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section");
        (data.pos(), data.len())
    };
    {
        let mut file = memfs.open(
            &blob_path,
            &crate::fs::FsOpenOptions::new().read(true).write(true),
        )?;
        file.seek(SeekFrom::Start(data_start))?;
        file.write_all(&vec![0u8; usize::try_from(data_len).unwrap_or(0)])?;
        file.sync_all()?;
    }

    let fs: Arc<dyn Fs> = memfs;
    let derived = super::derive_blob_frontier(&fs, &blob_path, 0)?;
    assert!(
        matches!(derived, super::BlobFrontier::Whole),
        "a fully allocated zeroed section is destroyed data, not a relocation \
         whose file removal lagged a crash, got {derived:?}",
    );
    Ok(())
}

/// Builds a one-blob tree with its manifest removed, ready for a repair.
#[expect(clippy::expect_used, reason = "test code")]
fn blob_tree_without_manifest(
    memfs: &std::sync::Arc<crate::fs::MemFs>,
    root: &std::path::Path,
) -> crate::Result<()> {
    use crate::fs::Fs;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};

    {
        let tree = match Config::new(
            root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        // Eight separated values, matching what `punch_and_corrupt_blob`
        // expects to find when a test damages this blob file.
        for i in 0..8u32 {
            tree.insert(
                format!("k{i:04}").as_bytes(),
                alloc::vec![b'a' + u8::try_from(i).expect("small i"); 64],
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }
    for e in memfs.read_dir(root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }
    Ok(())
}

/// A salvage replacement a crashed attempt left behind must not be admitted
/// as an ordinary blob file. It is fully written and checksum-valid, so the
/// scan cannot tell it from a real one — but no surviving table references it,
/// and a blob nothing references holds no reachable value. Admitting it would
/// strand a whole copy per failed attempt: repair cannot rebuild fragmentation
/// stats from a directory scan, and GC retires a file only once its recorded
/// stale bytes reach totals it would never get.
#[cfg(feature = "lz4")]
#[test]
fn repair_leaves_an_unreferenced_blob_out_of_the_manifest() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;

    let config = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    // A first attempt that fails AFTER publishing its replacement leaves a
    // fully written, checksum-valid blob under a fresh id that nothing points
    // at — exactly what the scan cannot tell apart from a real blob file.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Interrupted))
            .on_path(crate::file::CURRENT_VERSION_FILE),
    );
    assert!(
        config(Arc::new(fault)).repair().is_err(),
        "the faulted attempt must fail after producing its replacement",
    );
    assert!(
        memfs.exists(&blobs.join("1"))?,
        "the abandoned replacement is what this test is about",
    );
    // The attempt's own rewritten table never reached its name — it is still the
    // unpublished `{id}.repair-tmp`, which the retry drops — so the abandoned
    // blob is referenced by NOTHING, which is the state under test.
    assert!(
        memfs.exists(&super::repair_tmp_path(&root.join("tables").join("0")))?,
        "the unpublished replacement is what leaves the blob unreferenced",
    );

    // The retry salvages the untouched original again, into yet another id.
    config(memfs.clone()).repair()?;

    let tree = config(memfs.clone()).open()?;
    assert_eq!(
        tree.blob_file_count(),
        1,
        "only the blob the surviving table references belongs in the manifest; \
         admitting the abandoned copy would pin it forever, since repair cannot \
         rebuild the fragmentation stats GC needs to retire it",
    );
    assert!(
        !memfs.exists(&blobs.join("1"))?,
        "the unreferenced copy is removed by the repair that omits it",
    );
    Ok(())
}

/// A superseded original that cannot be removed fails the repair. Left in
/// `blobs/` it is outside the committed manifest, so the next open classifies
/// it as an orphan and must remove it — hitting the same refusal and failing
/// to open. Reporting success for a tree that will not open is the one
/// outcome recovery must never produce, so the failure propagates and a retry
/// finishes the job once the filesystem is fixed.
#[cfg(feature = "lz4")]
#[test]
fn repair_fails_when_a_superseded_blob_original_cannot_be_removed() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;

    // The removal is refused persistently, exactly as the next open's orphan
    // sweep would be refused.
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );

    let result = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .repair();
    assert!(
        result.is_err(),
        "a superseded original that cannot be removed must fail the repair, \
         not be reported as a success the next open cannot honour: {result:?}",
    );
    Ok(())
}

/// The salvaged replacement takes a FRESH blob id while the damaged original
/// keeps its own, so neither file is ever written over the other. The original
/// is removed only after the manifest is committed, which is what lets a failed
/// attempt leave it in place for the retry.
#[test]
fn a_salvaged_blob_takes_a_fresh_id_and_the_original_is_removed() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone())
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .repair()?;
    assert_eq!(
        report.blob_files_salvaged.len(),
        1,
        "the damaged blob was salvaged: {report:?}",
    );
    assert!(
        memfs.exists(&blobs.join("1"))?,
        "the replacement took the next free id rather than the original's",
    );
    assert!(
        !memfs.exists(&blobs.join("0"))?,
        "the superseded original is removed once the manifest is committed",
    );
    Ok(())
}

/// A recovered SST whose blob handles point BELOW a punched blob file's
/// derived live-data frontier must not be published as-is — a read through
/// such a handle dereferences the punched (zeroed) prefix and fails. Instead
/// of setting the whole table aside (discarding its intact entries), repair
/// REWRITES it: the sub-frontier entries are dropped (their records were
/// relocated elsewhere and this pre-relocation survivor is stale), every
/// other entry is preserved. Reachable when a crash leaves a pre-relocation
/// SST file on disk after the relocation's punch ran and the manifest is lost.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn repair_rewrites_tables_with_handles_below_a_blob_frontier() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    let root = std::path::absolute("/db")?;

    {
        let tree = match Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?
        {
            crate::AnyTree::Blob(t) => t,
            crate::AnyTree::Standard(_) => panic!("expected blob tree"),
        };
        for i in 0..8u32 {
            tree.insert(format!("k{i:04}").as_bytes(), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Simulate the crash aftermath of a blob relocation: the blob's first
    // frame is punched (its value lives elsewhere now) while a pre-relocation
    // SST still holds a handle into that prefix, and the manifest is gone.
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let blob_path = memfs
        .read_dir(&blobs)?
        .into_iter()
        .find(|e| !e.is_dir)
        .expect("one blob file")
        .path;
    let entries: Vec<_> = crate::vlog::BlobFileScanner::new(&blob_path, &*fs_dyn, 0)?
        .collect::<crate::Result<Vec<_>>>()?;
    assert!(entries.len() >= 2, "several frames written");
    let frontier = entries.first().expect("first frame").frame_end;
    let data_start = {
        let mut file = fs_dyn.open(&blob_path, &crate::fs::FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        reader
            .toc()
            .section(b"data")
            .expect("blob file has a data section")
            .pos()
    };
    memfs.punch_hole(&blob_path, data_start, frontier - data_start)?;
    for e in memfs.read_dir(&root)? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            memfs.remove_file(&e.path)?;
        }
    }

    let report = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&memfs) as Arc<dyn Fs>)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .repair()?;

    assert_eq!(
        report.recovered, 1,
        "the table survives, rewritten with its stale handles dropped: {report:?}",
    );
    assert_eq!(
        report.salvaged, 1,
        "the rewrite runs through the salvage pipeline and counts as such: {report:?}",
    );

    // Reopen: the punched-prefix record's key is absent (its entry was
    // dropped — the record was relocated elsewhere and this survivor is
    // stale), every later record still reads.
    let tree = match Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected blob tree"),
    };
    assert_eq!(
        tree.get(b"k0000", crate::MAX_SEQNO)?,
        None,
        "the sub-frontier record's key reads as absent, never as an error",
    );
    for i in 1..8u32 {
        assert!(
            tree.get(format!("k{i:04}").as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "record k{i:04} above the frontier must survive the rewrite",
        );
    }
    Ok(())
}

/// A persistently unreadable blob file is left OUT of the rebuilt manifest and
/// queued for removal: a file both omitted and left in place is an orphan the
/// next open must sweep, and an open that cannot sweep it fails. The scan does
/// not touch it, so a crash before the commit leaves the directory exactly as
/// the retry expects to find it.
#[test]
fn blob_recovery_discards_a_persistently_unreadable_blob_file() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::io::Write;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    {
        // A parseable id whose content is not a blob file (no SFA trailer):
        // persistently unreadable, not transient.
        let mut f = memfs.open(
            &blobs.join("3"),
            &crate::fs::FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(b"not a blob file")?;
    }

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone());

    let recovery = super::recover_blob_files(&config)?;
    let (files, unreadable, discard) = (recovery.files, recovery.unreadable, recovery.discard);
    assert!(files.is_empty(), "nothing recoverable");
    assert_eq!(unreadable.len(), 1, "the bad blob is reported");
    assert_eq!(
        discard.iter().map(|(p, _)| p).collect::<Vec<_>>(),
        vec![&blobs.join("3")],
        "the unreadable blob is queued for removal after the commit",
    );
    assert!(
        memfs.exists(&blobs.join("3"))?,
        "the scan removes nothing itself",
    );
    Ok(())
}

/// Two directory entries that parse to the SAME blob id (`1` and `01`) but are
/// DISTINCT physical files: the duplicate is reported and recorded for removal,
/// never silently kept — the rebuilt manifest records one checksum per id, while
/// a leftover stale duplicate would race the kept file for reads on the next
/// open (directory iteration order picks the physical file). The canonical
/// name (the writer's own `id.to_string()` spelling) is the one kept. The scan
/// itself does not touch the file: the removal belongs after the commit.
#[test]
fn blob_recovery_discards_a_duplicate_blob_id() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;

    let fs_dyn: Arc<dyn Fs> = memfs.clone();
    // Both are VALID blob files with different content, parsing to id 1.
    for (name, val) in [("1", b"canonical".as_slice()), ("01", b"stale-dup")] {
        let mut w = crate::vlog::blob_file::writer::Writer::new(blobs.join(name), 1, 0, &*fs_dyn)?;
        w.write(b"k", 1, val)?;
        w.finish()?;
    }

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs.clone());

    let recovery = super::recover_blob_files(&config)?;
    let (files, unreadable, discard) = (recovery.files, recovery.unreadable, recovery.discard);
    assert_eq!(files.len(), 1, "one blob file per id");
    assert_eq!(
        unreadable.len(),
        1,
        "the displaced duplicate is reported: {unreadable:?}"
    );
    assert!(
        memfs.exists(&blobs.join("1"))?,
        "the canonical spelling is the kept file"
    );
    assert_eq!(
        discard.iter().map(|(p, _)| p).collect::<Vec<_>>(),
        vec![&blobs.join("01")],
        "the duplicate is queued for removal once the manifest is durable",
    );
    assert!(
        memfs.exists(&blobs.join("01"))?,
        "the scan itself removes nothing: a crash here must leave the directory \
         exactly as the retry expects to find it",
    );
    // The recorded checksum matches the KEPT file.
    let kept = crate::Checksum::from_raw(super::compute_table_checksum(
        &*config.fs,
        &blobs.join("1"),
    )?);
    assert_eq!(
        files.first().map(crate::vlog::BlobFile::checksum),
        Some(kept),
        "the manifest checksum must describe the kept canonical file"
    );
    Ok(())
}

/// A TRANSIENT read failure while reading a blob file (the frontier probe or
/// the streaming checksum) must PROPAGATE, not land in `unreadable`: recording
/// it there installs a manifest that omits the blob, and the next open's
/// orphan sweep then deletes the healthy file — permanent value loss from a
/// one-shot I/O fault. The table-recovery path already propagates; the blob
/// scan must match it.
#[test]
fn blob_recovery_propagates_a_transient_checksum_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    memfs.create_dir_all(&blobs)?;
    {
        // A VALID blob file, so the transient fault is the only obstacle (a
        // garbage file would classify persistent-unreadable before any read
        // could be faulted).
        let fs_dyn: Arc<dyn Fs> = memfs.clone();
        let mut w = crate::vlog::blob_file::writer::Writer::new(blobs.join("0"), 0, 0, &*fs_dyn)?;
        w.write(b"k", 1, b"blob bytes")?;
        w.finish()?;
    }

    // Fault the per-file streaming reads (frontier probe + whole-file
    // checksum) with a RETRYABLE kind. `WouldBlock` (EAGAIN), not
    // `Interrupted` (EINTR): both classify transient, but `std::io`'s
    // `read_exact` transparently retries `Interrupted`, so a permanently
    // armed EINTR would spin the probe's buffered reads forever instead of
    // surfacing.
    let fault = FaultFs::new(memfs.as_ref().clone());
    fault
        .injector()
        .arm(FaultRule::new(FaultOp::Read, Fault::Error(ErrorKind::WouldBlock)).on_path("blobs"));

    let config = Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fault);

    let result = super::recover_blob_files(&config);
    assert!(
        result.is_err(),
        "a transient blob checksum failure must propagate for a retry, not be \
         recorded as unreadable: {:?}",
        result.map(|r| (r.files.len(), r.unreadable)),
    );
    Ok(())
}

/// A crash BEFORE the manifest commit leaves `{id}.repair-tmp` beside the
/// source the still-current manifest describes. The manifest names that id in
/// this state too, so id membership alone cannot tell an unpublished build
/// from a committed swap — and renaming the temp (possibly truncated mid-write)
/// over the source would destroy the one file the manifest actually names. The
/// open must recognize that the manifest's checksum describes the SOURCE,
/// discard the temp, and serve the source untouched.
#[test]
fn open_discards_an_uncommitted_repair_replacement() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::io::Write;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
    };

    {
        let crate::AnyTree::Standard(tree) = config().open()? else {
            panic!("expected a standard tree");
        };
        tree.insert(b"k", b"v", 1);
        tree.flush_active_memtable(0)?;
    }

    // The mid-build crash: a temp no committed manifest describes.
    let sst = root.join("tables").join("0");
    let tmp = super::repair_tmp_path(&sst);
    {
        let mut file = memfs.open(&tmp, &FsOpenOptions::new().write(true).create_new(true))?;
        file.write_all(b"half-built garbage")?;
    }

    let crate::AnyTree::Standard(tree) = config().open()? else {
        panic!("expected a standard tree");
    };
    assert_eq!(
        tree.get(b"k", u64::MAX)?.as_deref(),
        Some(b"v".as_ref()),
        "the source the manifest describes must survive the leftover temp",
    );
    assert!(
        !memfs.exists(&tmp)?,
        "the unpublished replacement is garbage and is swept",
    );
    Ok(())
}

/// The same crash state resolved by a RE-RUN of the repair instead of an open:
/// the pre-scan sweep must not swap the unpublished temp in either — the
/// manifest checksum describes the source, so the temp is dropped and the
/// repair re-derives everything from the untouched source.
#[test]
fn repair_discards_an_uncommitted_repair_replacement() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use std::io::Write;
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    let config = || {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(memfs.clone())
    };

    {
        let crate::AnyTree::Standard(tree) = config().open()? else {
            panic!("expected a standard tree");
        };
        tree.insert(b"k", b"v", 1);
        tree.flush_active_memtable(0)?;
    }

    let sst = root.join("tables").join("0");
    let tmp = super::repair_tmp_path(&sst);
    {
        let mut file = memfs.open(&tmp, &FsOpenOptions::new().write(true).create_new(true))?;
        file.write_all(b"half-built garbage")?;
    }

    let report = config().repair()?;
    assert_eq!(
        report.unreadable, 0,
        "nothing is damaged: the source is intact and the temp is not a table",
    );

    let crate::AnyTree::Standard(tree) = config().open()? else {
        panic!("expected a standard tree");
    };
    assert_eq!(
        tree.get(b"k", u64::MAX)?.as_deref(),
        Some(b"v".as_ref()),
        "the intact source must survive the repair re-run",
    );
    assert!(
        !memfs.exists(&tmp)?,
        "the unpublished replacement is garbage and is swept",
    );
    Ok(())
}

/// Manifest-loss repair orders L0 by the persisted recency key, not by raw
/// table id. A compaction output's id is allocated when the compaction starts
/// writing, while a newer flush with a LOWER id can install first (and an
/// intra-L0 output is appended at the BACK of L0 regardless of its id), so id
/// order would put the output's OLDER content in front — and a read at a
/// caller-chosen tied seqno would serve the superseded value.
#[test]
fn repair_orders_l0_by_recency_key_not_table_id() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{AbstractTree, Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // Table 2: a flush — the NEWER write of `k` at the tied seqno.
    {
        let mut w = Writer::new(tables.join("2"), 2, 0, Arc::clone(&fs))?;
        w.write(InternalValue::from_components(
            b"k",
            b"new",
            10,
            ValueType::Value,
        ))?;
        assert!(w.finish()?.is_some(), "the flush table is non-empty");
    }
    // Table 5: an intra-L0 compaction output of OLDER flushes (recency 1),
    // carrying the superseded write of `k` at the same caller-chosen seqno.
    {
        let mut w = Writer::new(tables.join("5"), 5, 0, Arc::clone(&fs))?.use_recency(Some(1));
        w.write(InternalValue::from_components(
            b"k",
            b"old",
            10,
            ValueType::Value,
        ))?;
        assert!(w.finish()?.is_some(), "the output table is non-empty");
    }

    let config = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    };
    config().repair()?;

    let crate::AnyTree::Standard(tree) = config().open()? else {
        panic!("expected a standard tree");
    };
    assert_eq!(
        tree.get(b"k", u64::MAX)?.as_deref(),
        Some(b"new".as_ref()),
        "the flush's newer write must shadow the compaction output's superseded \
         one, exactly as the live tree ordered them",
    );
    Ok(())
}

/// The counterpart committed case: a repair that dies AFTER `persist_version`
/// but before the swap leaves a temp the committed manifest DOES describe (its
/// entry carries the replacement's digest). The next open must finish that
/// swap, not discard the only copy of what the manifest names.
#[cfg(feature = "lz4")]
#[test]
fn open_finishes_a_committed_repair_swap() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::io::ErrorKind;
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::sync::Arc;

    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;
    blob_tree_without_manifest(&memfs, &root)?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    punch_and_corrupt_blob(&memfs, &blobs.join("0"))?;

    let config = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(16),
        ))
    };

    // The blob rewrite publishes the referencing table's replacement at
    // `{id}.repair-tmp` and swaps it only after the commit; failing the swap's
    // rename persistently is the post-commit crash. Rename faults match the
    // DESTINATION path — the table's own name, spelled with the platform's
    // separators (a literal "tables/0" would never match on Windows).
    let swap_dest = root.join("tables").join("0");
    let fault = FaultFs::new((*memfs).clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::PermissionDenied))
            .on_path(swap_dest.to_string_lossy().into_owned()),
    );
    assert!(
        config(Arc::new(fault)).repair().is_err(),
        "a swap the filesystem refuses must fail the repair after its commit",
    );
    let tmp = super::repair_tmp_path(&root.join("tables").join("0"));
    assert!(
        memfs.exists(&tmp)?,
        "the committed replacement is still at its temp name",
    );

    // The open resolves it from the committed manifest alone: the entry's
    // digest matches the temp, so the swap is finished, not discarded.
    let tree = config(memfs.clone()).open()?;
    assert!(
        !memfs.exists(&tmp)?,
        "the pending swap is finished by the open",
    );
    // Records 0-1 are punched and the last is corrupt, so a mid-range key is
    // a guaranteed survivor of the salvage.
    assert!(
        tree.get(b"k0004", u64::MAX)?.is_some(),
        "the salvaged content the manifest describes is served",
    );
    Ok(())
}
