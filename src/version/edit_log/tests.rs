use super::super::edit::{ChangedLevel, TableDesc, VersionEdit};
use super::*;
use crate::config::ManifestRecoveryMode;
use crate::fs::StdFs;

fn edit(id: u64) -> VersionEdit {
    VersionEdit {
        new_version_id: id,
        changed_levels: vec![ChangedLevel {
            level: 0,
            runs: vec![vec![TableDesc {
                id,
                checksum: u128::from(id) * 7,
                global_seqno: id * 10,
                recency: id * 3,
            }]],
        }],
        ..Default::default()
    }
}

/// A log named by a bare file name lies in the current directory: its first
/// append syncs that directory as `.`, the name every backend accepts, not
/// as the empty parent the name has, so its entry survives a crash.
#[test]
fn the_first_append_to_a_log_in_the_current_directory_syncs_it() -> crate::Result<()> {
    let fs = crate::fs::CrashFs::new(crate::fs::MemFs::new());
    let path = Path::new("edits-0");
    let mut scratch = Vec::new();
    append_edit(&fs, path, &edit(1), &mut scratch, SyncMode::Normal, true)?;

    fs.crash();
    assert_eq!(
        replay_log(&fs, path, ManifestRecoveryMode::AbsoluteConsistency)?,
        vec![edit(1)]
    );
    Ok(())
}

/// A log created over one that holds durable edits is empty after a power
/// loss: the edits of the old file never replay over the snapshot the new
/// log belongs to.
#[test]
fn a_log_created_over_durable_edits_stays_empty_through_a_power_loss() -> crate::Result<()> {
    let fs = crate::fs::CrashFs::new(crate::fs::MemFs::new());
    fs.create_dir_all(Path::new("/db"))?;
    let path = log_path(Path::new("/db"), 0);
    let mut scratch = Vec::new();
    append_edit(&fs, &path, &edit(1), &mut scratch, SyncMode::Full, true)?;

    create_log(&fs, Path::new("/db"), 0, SyncMode::Full)?;
    fs.sync_directory(Path::new("/db"))?;
    fs.crash();
    assert_eq!(
        replay_log(&fs, &path, ManifestRecoveryMode::AbsoluteConsistency)?,
        Vec::new()
    );
    Ok(())
}

/// A log whose emptying failed to sync is synced on the retry, though it reads
/// empty by then: the retry must not take the unsynced length for a durable
/// one, or a power loss brings the old edits back.
#[test]
fn a_log_whose_emptying_failed_to_sync_is_synced_on_the_retry() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    let crash = crate::fs::CrashFs::new(crate::fs::MemFs::new());
    crash.create_dir_all(Path::new("/db"))?;
    let path = log_path(Path::new("/db"), 0);
    let mut scratch = Vec::new();
    append_edit(&crash, &path, &edit(1), &mut scratch, SyncMode::Full, true)?;

    let fs = FaultFs::new(crash.clone());
    fs.injector().arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(path.to_string_lossy())
            .times(1),
    );
    assert!(
        create_log(&fs, Path::new("/db"), 0, SyncMode::Full).is_err(),
        "self-check: the first emptying fails its sync"
    );
    create_log(&fs, Path::new("/db"), 0, SyncMode::Full)?;
    crash.sync_directory(Path::new("/db"))?;
    crash.crash();
    assert_eq!(
        replay_log(&crash, &path, ManifestRecoveryMode::AbsoluteConsistency)?,
        Vec::new()
    );
    Ok(())
}

/// A probe of the log's name that fails for another reason than its absence
/// fails the creation: whether a log is there to empty is not known.
#[test]
fn a_failed_probe_of_the_log_fails_its_creation() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    let fs = FaultFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/db"))?;
    fs.injector().arm(
        FaultRule::new(
            FaultOp::Metadata,
            Fault::Error(crate::io::ErrorKind::PermissionDenied),
        )
        .on_path("edits-0"),
    );
    let Err(crate::Error::Io(e)) = create_log(&fs, Path::new("/db"), 0, SyncMode::Full) else {
        panic!("the probe's failure is the creation's");
    };
    assert_eq!(e.kind(), crate::io::ErrorKind::PermissionDenied);
    fs.injector().clear();
    assert!(
        !fs.exists(&log_path(Path::new("/db"), 0))?,
        "no log is made"
    );
    Ok(())
}

/// A first append whose directory sync fails writes no edit: a record left
/// in the log would be made durable by the next append's sync, though the
/// operation it records was reported failed.
#[test]
fn a_first_append_whose_directory_sync_fails_writes_nothing() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};

    let fs = FaultFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/db"))?;
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    let path = Path::new("/db/edits-0");
    let mut scratch = Vec::new();
    assert!(append_edit(&fs, path, &edit(1), &mut scratch, SyncMode::Normal, true).is_err());
    assert_eq!(
        replay_log(&fs, path, ManifestRecoveryMode::AbsoluteConsistency)?,
        Vec::new(),
        "the failed append left no record"
    );
    Ok(())
}

/// An append syncs the log's data, not its full metadata: a record and the
/// length that reaches it are what replay reads, and `fdatasync` makes both
/// durable, so a backend refusing every full sync still takes the append.
#[test]
fn an_append_syncs_the_data_of_the_log_not_its_metadata() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};

    let fs = FaultFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/db"))?;
    fs.injector().arm(FaultRule::new(
        FaultOp::SyncAll,
        Fault::Error(crate::io::ErrorKind::Other),
    ));
    let path = Path::new("/db/edits-0");
    let mut scratch = Vec::new();
    append_edit(&fs, path, &edit(1), &mut scratch, SyncMode::Normal, true)?;
    append_edit(&fs, path, &edit(2), &mut scratch, SyncMode::Normal, false)?;
    assert_eq!(
        replay_log(&fs, path, ManifestRecoveryMode::AbsoluteConsistency)?,
        alloc::vec![edit(1), edit(2)],
    );
    Ok(())
}

#[test]
fn append_then_replay_roundtrips_all_edits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-0");
    let mut scratch = Vec::new();
    let edits: Vec<VersionEdit> = (1..=4).map(edit).collect();
    for e in &edits {
        append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal, false).expect("append");
    }
    let replayed =
        replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency).expect("replay");
    assert_eq!(replayed, edits, "append+replay must round-trip in order");
}

/// An edit naming more tables than one record holds is declined before the
/// log is touched, so the caller can rotate instead and the log it would have
/// torn stays as it was.
#[test]
fn an_edit_too_large_for_one_record_writes_nothing() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("edits-0");
    let mut scratch = Vec::new();
    let table = |id| TableDesc {
        id,
        checksum: 0,
        global_seqno: 0,
        recency: id,
    };
    let wide = VersionEdit {
        new_version_id: 1,
        changed_levels: vec![ChangedLevel {
            level: 0,
            runs: vec![(0..4_000).map(table).collect()],
        }],
        ..Default::default()
    };

    let appended = append_edit(&StdFs, &path, &wide, &mut scratch, SyncMode::Normal, false)?;
    assert_eq!(appended, None);
    assert!(!path.exists(), "a declined edit must not create the log");
    Ok(())
}

#[test]
fn replay_absent_log_is_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-missing");
    assert!(
        replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency)
            .expect("replay")
            .is_empty()
    );
    assert_eq!(log_size(&StdFs, &path).expect("size"), 0);
}

/// Appends `count` clean edits, then truncates the file to `clean + 5`
/// bytes past the durable prefix so the trailing record is partial
/// (a power-loss-interrupted append). Returns the path.
fn log_with_torn_tail(dir: &std::path::Path, count: u64) -> std::path::PathBuf {
    let path = dir.join("edits-torn");
    let mut scratch = Vec::new();
    for i in 1..=count {
        append_edit(
            &StdFs,
            &path,
            &edit(i),
            &mut scratch,
            SyncMode::Normal,
            false,
        )
        .expect("append");
    }
    let clean = log_size(&StdFs, &path).expect("size");
    append_edit(
        &StdFs,
        &path,
        &edit(count + 1),
        &mut scratch,
        SyncMode::Normal,
        false,
    )
    .expect("append");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    f.set_len(clean + 5).expect("truncate");
    drop(f);
    path
}

#[test]
fn torn_tail_record_is_dropped_on_replay() {
    // Lenient mode: a partial trailing record (simulated power loss mid-append)
    // is dropped, keeping the durable prefix.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = log_with_torn_tail(dir.path(), 2);
    let replayed = replay_log(
        &StdFs,
        &path,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
    )
    .expect("replay");
    assert_eq!(
        replayed,
        vec![edit(1), edit(2)],
        "torn tail dropped, clean prefix kept",
    );
}

#[test]
fn torn_tail_record_aborts_under_strict() {
    // AbsoluteConsistency (strict): a partial trailing record is NOT silently
    // rolled back — replay surfaces TornManifestEditLog so an operator must
    // truncate the tail (Tree::repair) before the tree opens.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = log_with_torn_tail(dir.path(), 2);
    let err = replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency)
        .expect_err("strict must reject torn tail");
    assert!(
        matches!(err, crate::Error::TornManifestEditLog { kind: "truncated" }),
        "expected TornManifestEditLog(truncated), got {err:?}",
    );
}

#[test]
fn clean_log_replays_under_strict() {
    // A pristine log (every record fully fsynced) ends in a clean record
    // boundary, which the trailing read sees as EOF — byte-identical to a
    // crash exactly at a boundary. Strict mode MUST tolerate this, else every
    // healthy open would fail under the default AbsoluteConsistency mode.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-clean");
    let mut scratch = Vec::new();
    let edits: Vec<VersionEdit> = (1..=3).map(edit).collect();
    for e in &edits {
        append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal, false).expect("append");
    }
    let replayed = replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency)
        .expect("strict must accept a clean log");
    assert_eq!(replayed, edits, "clean log replays fully under strict");
}

#[test]
fn checksum_mismatch_tail_aborts_in_every_mode() {
    // A fully-framed trailing record whose payload bit-rotted (length and
    // digest intact, bytes flipped) is corruption of committed bytes, not an
    // unacknowledged write, so every mode rejects it.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-bitrot");
    let mut scratch = Vec::new();
    for i in 1..=3 {
        append_edit(
            &StdFs,
            &path,
            &edit(i),
            &mut scratch,
            SyncMode::Normal,
            false,
        )
        .expect("append");
    }
    // Flip the last payload byte of the final (fully written) record.
    let mut bytes = std::fs::read(&path).expect("read");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    for mode in [
        ManifestRecoveryMode::AbsoluteConsistency,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
    ] {
        let err = replay_log(&StdFs, &path, mode).expect_err("must reject committed bit-rot");
        assert!(
            matches!(
                err,
                crate::Error::TornManifestEditLog {
                    kind: "checksum-mismatch"
                }
            ),
            "expected TornManifestEditLog(checksum-mismatch) under {mode:?}, got {err:?}",
        );
    }
}

#[test]
fn log_size_grows_with_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-size");
    let mut scratch = Vec::new();
    let s0 = log_size(&StdFs, &path).expect("size");
    append_edit(
        &StdFs,
        &path,
        &edit(1),
        &mut scratch,
        SyncMode::Normal,
        false,
    )
    .expect("append");
    let s1 = log_size(&StdFs, &path).expect("size");
    append_edit(
        &StdFs,
        &path,
        &edit(2),
        &mut scratch,
        SyncMode::Normal,
        false,
    )
    .expect("append");
    let s2 = log_size(&StdFs, &path).expect("size");
    assert_eq!(s0, 0);
    assert!(s1 > s0 && s2 > s1, "log grows with each appended edit");
}
