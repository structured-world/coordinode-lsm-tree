use super::*;
use crate::comparator::default_comparator;
use test_log::test;

#[test]
fn of_run_without_a_loss_or_a_filter_verdict_keeps() {
    // The watermark alone says nothing: a run handed one that collected
    // nothing owes older snapshots nothing either.
    assert!(matches!(
        RetentionEffect::of_run(false, false, 500),
        RetentionEffect::Keep
    ));
    assert!(matches!(
        RetentionEffect::of_run(false, false, 0),
        RetentionEffect::Keep
    ));
}

#[test]
fn of_run_with_a_loss_below_the_watermark_reports_that_watermark() {
    assert!(matches!(
        RetentionEffect::of_run(false, true, 500),
        RetentionEffect::GcBelow(500)
    ));
    // `GcBelow(0)` is `Keep` downstream, so a watermark of 0 stays harmless
    // even when the run reports a loss it could only have made at 0.
    assert!(matches!(
        RetentionEffect::of_run(false, true, 0),
        RetentionEffect::GcBelow(0)
    ));
}

#[test]
fn of_run_with_a_filter_verdict_drops_data_whatever_else_happened() {
    // A filter acts regardless of any watermark, so its verdict outranks both
    // the watermark and whether the folds collected anything.
    assert!(matches!(
        RetentionEffect::of_run(true, false, 0),
        RetentionEffect::DropsData
    ));
    assert!(matches!(
        RetentionEffect::of_run(true, true, 500),
        RetentionEffect::DropsData
    ));
}

fn super_version(id: u64, floor: SeqNo, seqno: SeqNo) -> SuperVersion {
    SuperVersion {
        active_memtable: Arc::new(Memtable::new(0, default_comparator())),
        sealed_memtables: Arc::default(),
        version: Version::new(id, crate::TreeType::Standard).with_retention_floor(floor),
        seqno,
    }
}

fn history_at_floor(floor: SeqNo) -> SuperVersions {
    let version = Version::new(7, crate::TreeType::Standard).with_retention_floor(floor);
    SuperVersions::new(
        version,
        &default_comparator(),
        SyncMode::Normal,
        7,
        1024 * 1024,
    )
}

fn floor(history: &SuperVersions) -> SeqNo {
    history.latest_version_ref().version.retention_floor()
}

fn assert_refused(history: &SuperVersions, requested: SeqNo, floor: SeqNo) {
    match history.get_version_for_snapshot(requested) {
        Err(crate::Error::SnapshotBelowRetention {
            requested: got,
            oldest_retained,
        }) => {
            assert_eq!(got, requested);
            assert_eq!(oldest_retained, floor);
        }
        Err(other) => panic!("snapshot {requested}: wrong error {other:?}"),
        Ok(version) => panic!(
            "snapshot {requested} must be refused, got version #{}",
            version.version.id()
        ),
    }
}

#[test]
fn snapshot_above_the_floor_is_served_by_the_current_version() -> crate::Result<()> {
    // Every snapshot above the floor reads the current version, however far
    // below the current install it sits.
    let history = history_at_floor(10);
    for snapshot in [11, 12, 1_000, SeqNo::MAX] {
        assert_eq!(history.get_version_for_snapshot(snapshot)?.version.id(), 7);
    }
    Ok(())
}

#[test]
fn snapshot_at_or_below_the_floor_is_refused_naming_the_floor() {
    let history = history_at_floor(10);
    for requested in [1, 9, 10] {
        assert_refused(&history, requested, 10);
    }
    assert_eq!(floor(&history), 10);
}

#[test]
fn snapshot_zero_is_served_below_any_floor() -> crate::Result<()> {
    // Nothing is visible at seqno 0 whatever the file set, so it is served
    // rather than refused, however high the floor.
    let history = history_at_floor(10);
    assert_eq!(history.get_version_for_snapshot(0)?.version.id(), 7);
    Ok(())
}

#[test]
fn install_raising_the_floor_moves_the_live_boundary_at_once() -> crate::Result<()> {
    // The boundary is the CURRENT version's floor: an install that raises it
    // refuses the snapshots below it straight away, with no older version left
    // behind to answer them.
    let mut history = history_at_floor(0);
    assert_eq!(history.get_version_for_snapshot(5)?.version.id(), 7);

    history.append_version(super_version(8, 20, 30));
    assert_eq!(floor(&history), 20);
    assert_refused(&history, 5, 20);
    assert_refused(&history, 20, 20);
    assert_eq!(history.get_version_for_snapshot(21)?.version.id(), 8);
    Ok(())
}

#[test]
fn replaced_version_lives_on_in_the_reader_that_resolved_it() -> crate::Result<()> {
    // A reader's clone is its own hold: the history moving on does not take
    // the version it resolved away from it.
    let mut history = history_at_floor(0);
    let reader = history.get_version_for_snapshot(SeqNo::MAX)?;

    history.append_version(super_version(8, 0, 30));
    assert_eq!(history.latest_version_ref().version.id(), 8);
    assert_eq!(
        reader.version.id(),
        7,
        "the reader still reads its own version"
    );
    Ok(())
}

/// A history over `fs` at `/tree`, its snapshot `v0` installed, whose log
/// is far below the rotation threshold.
fn installed_history(fs: &dyn crate::fs::Fs) -> crate::Result<SuperVersions> {
    let folder = crate::path::Path::new("/tree");
    fs.create_dir_all(folder)?;
    let version = Version::new(0, crate::TreeType::Standard);
    crate::version::persist_version(
        folder,
        &version,
        default_comparator().name(),
        fs,
        Arc::default(),
        None,
        SyncMode::Normal,
    )?;
    Ok(SuperVersions::new(
        version,
        &default_comparator(),
        SyncMode::Normal,
        0,
        1024 * 1024,
    ))
}

fn persist_next(history: &mut SuperVersions, fs: &dyn crate::fs::Fs) -> crate::Result<()> {
    let prior = Version::new(0, crate::TreeType::Standard);
    let next = Version::new(1, crate::TreeType::Standard);
    history.persist_change(
        crate::path::Path::new("/tree"),
        &prior,
        &next,
        fs,
        Arc::default(),
        None,
        SyncMode::Normal,
    )
}

/// After a rotation failed, the next install rotates even when its edit
/// would fit the current log: the failed one may have installed its
/// snapshot on disk, and an edit appended to the older log would then be
/// lost at recovery, which replays the newer snapshot's log.
#[test]
fn the_install_after_a_failed_rotation_rotates_instead_of_appending() -> crate::Result<()> {
    let fs = crate::fs::MemFs::new();
    let mut history = installed_history(&fs)?;
    history.rotation_pending = true;

    persist_next(&mut history, &fs)?;
    assert_eq!(history.snapshot_id, 1, "the install rotated");
    assert!(!history.rotation_pending, "a successful rotation clears it");
    assert_eq!(
        crate::version::recovery::installed_snapshot(crate::path::Path::new("/tree"), &fs)?,
        Some(1)
    );
    Ok(())
}

/// A rotation whose directory sync fails leaves its outcome unknown, so it
/// marks the next install to rotate, and that install replaces what the
/// failed one left and installs.
#[test]
fn a_rotation_failing_at_its_directory_sync_marks_the_next_to_rotate() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    let fs = FaultFs::new(MemFs::new());
    let mut history = installed_history(&fs)?;
    history.log_rotate_bytes = 0;
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );

    assert!(persist_next(&mut history, &fs).is_err());
    assert!(history.rotation_pending, "the next install must rotate");
    assert_eq!(history.snapshot_id, 0, "the history stays on its snapshot");

    persist_next(&mut history, &fs)?;
    assert!(!history.rotation_pending);
    assert_eq!(history.snapshot_id, 1);
    assert_eq!(
        crate::version::recovery::installed_snapshot(crate::path::Path::new("/tree"), &fs)?,
        Some(1)
    );
    Ok(())
}

/// Outputs kept after an install of unknown outcome are superseded by the
/// first rotation that succeeds after that install. When another install
/// rotated before the outputs are handed over, they are superseded already
/// and go at once, not after one more rotation; otherwise they wait for it.
#[test]
fn kept_outputs_wait_only_for_the_rotation_after_their_install() {
    use crate::compaction::output_ledger::OutputLedger;

    let mut history = history_at_floor(0);
    // The failure's mark was cleared by a rotation before the handover.
    history.rotation_pending = false;
    history.keep_outputs(OutputLedger::default().keep_uninstalled());
    assert_eq!(
        history.take_superseded_outputs().len(),
        1,
        "superseded already"
    );

    history.rotation_pending = true;
    history.keep_outputs(OutputLedger::default().keep_uninstalled());
    assert!(
        history.take_superseded_outputs().is_empty(),
        "the rotation is next"
    );
    history.rotations += 1;
    history.rotation_pending = false;
    assert_eq!(history.take_superseded_outputs().len(), 1);
}

#[test]
fn new_history_seeds_the_floor_as_the_boundary() {
    // A history built from a recovered version reads with that version's
    // persisted floor, so a reopen draws the boundary the live tree drew.
    let history = history_at_floor(42);
    assert_eq!(floor(&history), 42);
    assert!(history.get_version_for_snapshot(42).is_err());
    assert!(history.get_version_for_snapshot(43).is_ok());
}
