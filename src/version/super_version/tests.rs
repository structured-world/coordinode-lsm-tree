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

/// A deferred tree on a fault-injecting backend with one flush pending.
fn deferred_tree_with_a_pending_flush(
    fs: &Arc<crate::fs::FaultFs<crate::fs::MemFs>>,
) -> crate::Result<crate::Tree> {
    use crate::config::FlushDurability;
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

    let AnyTree::Standard(tree) = Config::new(
        std::path::absolute("/db")?,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .flush_durability(FlushDurability::Deferred)
    .open()?
    else {
        panic!("a standard tree");
    };
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    Ok(tree)
}

/// A barrier that took the pending files before another one's sync of them
/// failed does not acknowledge them by a sync of its own. Guards against
/// concurrent barriers, each holding its copy of the list, one failing and
/// the other then naming the file in a manifest through a new handle.
#[test]
fn a_barrier_holding_the_files_before_a_failed_sync_does_not_sync_them() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};

    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let tree = deferred_tree_with_a_pending_flush(&fs)?;
    let held = tree.version_history.read().unsynced_files();

    fs.injector()
        .arm(FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other)).once());
    assert!(tree.persist_flushes().is_err(), "the sync fails");
    assert!(
        SuperVersions::sync_unsynced(
            &held,
            SyncMode::default(),
            crate::io_policy::SyncSchedule::SideBySide,
            (&*fs, crate::path::Path::new("/db")),
        )
        .is_err(),
        "the other barrier's copy does not sync the file"
    );
    Ok(())
}

/// A batch of pending files in which one file's sync fails marks that file
/// alone: the others sync and persist on the next barrier. Guards against a
/// batch failure being pinned on every file in it, which would keep flushes
/// whose data did reach the disk out of every persist.
#[test]
fn a_failed_sync_in_a_batch_marks_only_its_own_file() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    use crate::io_policy::SyncSchedule;

    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let tree = deferred_tree_with_a_pending_flush(&fs)?;
    tree.insert("b", "2", 1);
    tree.flush_active_memtable(0)?;
    let pending = tree.version_history.read().unsynced_files();
    let [first, second] = pending.as_slice() else {
        panic!("two pending tables");
    };
    let step = (&*fs as &dyn crate::fs::Fs, crate::path::Path::new("/db"));

    fs.injector().arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(first.as_flush_file().location().1.to_string_lossy())
            .once(),
    );
    assert!(
        SuperVersions::sync_unsynced(&pending, SyncMode::default(), SyncSchedule::Batched, step)
            .is_err()
    );
    assert!(
        SuperVersions::sync_unsynced(
            core::slice::from_ref(second),
            SyncMode::default(),
            SyncSchedule::Batched,
            step,
        )
        .is_ok(),
        "the file whose sync succeeded syncs again"
    );
    assert!(
        SuperVersions::sync_unsynced(
            core::slice::from_ref(first),
            SyncMode::default(),
            SyncSchedule::Batched,
            step,
        )
        .is_err(),
        "the file whose sync failed does not"
    );
    Ok(())
}

/// Synced one at a time, a failed sync marks its own file alone too, by its
/// place in the batch: the file before it synced and syncs again, the failed
/// one does not. Guards against the one-at-a-time schedule pinning a failure
/// on the first file of the batch.
#[test]
fn one_at_a_time_a_failed_sync_marks_only_its_own_file() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    use crate::io_policy::SyncSchedule;

    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let tree = deferred_tree_with_a_pending_flush(&fs)?;
    tree.insert("b", "2", 1);
    tree.flush_active_memtable(0)?;
    let pending = tree.version_history.read().unsynced_files();
    let [first, second] = pending.as_slice() else {
        panic!("two pending tables");
    };
    let step = (&*fs as &dyn crate::fs::Fs, crate::path::Path::new("/db"));

    fs.injector().arm(
        FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(second.as_flush_file().location().1.to_string_lossy())
            .once(),
    );
    assert!(
        SuperVersions::sync_unsynced(
            &pending,
            SyncMode::default(),
            SyncSchedule::OneAtATime,
            step
        )
        .is_err()
    );
    assert!(
        SuperVersions::sync_unsynced(
            core::slice::from_ref(first),
            SyncMode::default(),
            SyncSchedule::OneAtATime,
            step,
        )
        .is_ok(),
        "the file synced before the failure syncs again"
    );
    assert!(
        SuperVersions::sync_unsynced(
            core::slice::from_ref(second),
            SyncMode::default(),
            SyncSchedule::OneAtATime,
            step,
        )
        .is_err(),
        "the file whose sync failed does not"
    );
    Ok(())
}

/// Under barrier syncs, a device whose cache did not flush leaves the files
/// synced to it unproven: they are marked as failed, and a later barrier
/// does not take a sync of its own for proof. Guards against a failed flush
/// of a table's device clearing the table for the next persist to name.
#[test]
fn a_failed_device_flush_marks_the_files_on_that_device() -> crate::Result<()> {
    use crate::config::FlushDurability;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
    use crate::io_policy::SyncSchedule;
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let AnyTree::Standard(tree) = Config::new(
        std::path::absolute("/db")?,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs.clone())
    .sync_mode(SyncMode::Barrier)
    .flush_durability(FlushDurability::Deferred)
    .open()?
    else {
        panic!("a standard tree");
    };
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    let pending = tree.version_history.read().unsynced_files();
    let step = (&*fs as &dyn crate::fs::Fs, crate::path::Path::new("/db"));

    // The tables' device is not provably the manifest's: its cache is
    // flushed on its own, and the flush fails once.
    fs.injector().arm(
        FaultRule::new(FaultOp::VolumeId, Fault::Error(crate::io::ErrorKind::Other))
            .on_path("tables"),
    );
    fs.injector().arm(
        FaultRule::new(
            FaultOp::SyncDevice,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    let flushed = |syncs: Vec<(crate::path::PathBuf, crate::fs::SyncKind)>| {
        syncs
            .iter()
            .filter(|(_, kind)| *kind == crate::fs::SyncKind::Device)
            .count()
    };
    // The flush's own syncs, before what is counted.
    let _ = fs.injector().take_syncs();
    assert!(
        SuperVersions::sync_unsynced(&pending, SyncMode::Barrier, SyncSchedule::SideBySide, step)
            .is_err(),
        "the device flush fails"
    );
    assert_eq!(
        flushed(fs.injector().take_syncs()),
        1,
        "the failure is the device flush's"
    );
    assert!(
        SuperVersions::sync_unsynced(&pending, SyncMode::Barrier, SyncSchedule::SideBySide, step)
            .is_err(),
        "the table synced to that device is not taken as durable"
    );
    assert_eq!(
        flushed(fs.injector().take_syncs()),
        0,
        "the retry fails on the marked file, before any device flush"
    );
    Ok(())
}

/// A barrier that could not open a pending file synced nothing, so the next
/// one syncs and persists it. Guards against a transient failure to open
/// (descriptors run out) being taken for a failed sync, which no retry
/// could ever clear.
#[test]
fn a_file_that_could_not_be_opened_is_synced_by_the_next_barrier() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};

    let fs = Arc::new(FaultFs::new(MemFs::new()));
    let tree = deferred_tree_with_a_pending_flush(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(crate::io::ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    assert!(tree.persist_flushes().is_err(), "the open fails");
    tree.persist_flushes()?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(0));
    Ok(())
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
        Arc::default(),
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
        0,
    )?;
    Ok(SuperVersions::new(
        version,
        &default_comparator(),
        SyncMode::Normal,
        0,
        1024 * 1024,
        Arc::default(),
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

/// Outputs kept after an install of unknown outcome wait for a rotation that
/// counts them, even when another install rotated before they were handed
/// over: that one synced its snapshot without knowing where they lie.
#[test]
fn kept_outputs_wait_for_a_rotation_that_counts_them() -> crate::Result<()> {
    use crate::compaction::output_ledger::OutputLedger;

    let fs = crate::fs::MemFs::new();
    let mut history = installed_history(&fs)?;
    // The failure's mark was cleared by a rotation before the handover.
    history.rotation_pending = false;
    history.keep_outputs(OutputLedger::default().keep_uninstalled(0, None));
    assert_eq!(history.kept_outputs.len(), 1, "held");
    assert!(history.rotation_pending, "the next install rotates");

    persist_next(&mut history, &fs)?;
    assert!(history.kept_outputs.is_empty(), "the rotation removed them");
    assert!(!history.rotation_pending);
    Ok(())
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
