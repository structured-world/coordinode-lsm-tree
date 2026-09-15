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

#[test]
fn new_history_seeds_the_floor_as_the_boundary() {
    // A history built from a recovered version reads with that version's
    // persisted floor, so a reopen draws the boundary the live tree drew.
    let history = history_at_floor(42);
    assert_eq!(floor(&history), 42);
    assert!(history.get_version_for_snapshot(42).is_err());
    assert!(history.get_version_for_snapshot(43).is_ok());
}
