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
            }]],
        }],
        ..Default::default()
    }
}

#[test]
fn append_then_replay_roundtrips_all_edits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-0");
    let mut scratch = Vec::new();
    let edits: Vec<VersionEdit> = (1..=4).map(edit).collect();
    for e in &edits {
        append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal).expect("append");
    }
    let replayed =
        replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency).expect("replay");
    assert_eq!(replayed, edits, "append+replay must round-trip in order");
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
        append_edit(&StdFs, &path, &edit(i), &mut scratch, SyncMode::Normal).expect("append");
    }
    let clean = log_size(&StdFs, &path).expect("size");
    append_edit(
        &StdFs,
        &path,
        &edit(count + 1),
        &mut scratch,
        SyncMode::Normal,
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
        append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal).expect("append");
    }
    let replayed = replay_log(&StdFs, &path, ManifestRecoveryMode::AbsoluteConsistency)
        .expect("strict must accept a clean log");
    assert_eq!(replayed, edits, "clean log replays fully under strict");
}

#[test]
fn checksum_mismatch_tail_aborts_under_strict_and_tolerate_tail() {
    // A fully-framed trailing record whose payload bit-rotted (length and
    // digest intact, bytes flipped) is corruption of committed bytes, not an
    // unacknowledged write. AbsoluteConsistency AND TolerateCorruptedTailRecords
    // (writer-incomplete salvage only) must both reject it; only the
    // corruption-tolerant modes (PIT / SkipAny) drop it.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-bitrot");
    let mut scratch = Vec::new();
    for i in 1..=3 {
        append_edit(&StdFs, &path, &edit(i), &mut scratch, SyncMode::Normal).expect("append");
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

    let replayed = replay_log(&StdFs, &path, ManifestRecoveryMode::PointInTimeRecovery)
        .expect("PIT drops bit-rotted tail");
    assert_eq!(
        replayed,
        vec![edit(1), edit(2)],
        "PIT: bit-rotted tail dropped, prefix kept",
    );
}

#[test]
fn log_size_grows_with_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("edits-size");
    let mut scratch = Vec::new();
    let s0 = log_size(&StdFs, &path).expect("size");
    append_edit(&StdFs, &path, &edit(1), &mut scratch, SyncMode::Normal).expect("append");
    let s1 = log_size(&StdFs, &path).expect("size");
    append_edit(&StdFs, &path, &edit(2), &mut scratch, SyncMode::Normal).expect("append");
    let s2 = log_size(&StdFs, &path).expect("size");
    assert_eq!(s0, 0);
    assert!(s1 > s0 && s2 > s1, "log grows with each appended edit");
}
