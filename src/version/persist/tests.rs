use super::persist_version;
use crate::fs::{Fs, FsOpenOptions, MemFs, SyncMode};
use crate::path::Path;
use crate::version::{Version, recovery::installed_snapshot};
use alloc::sync::Arc;

/// Writes `bytes` to `path`, creating it.
fn put(fs: &MemFs, path: &Path, bytes: &[u8]) -> crate::Result<()> {
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(bytes)?;
    Ok(())
}

fn persist(fs: &MemFs, folder: &Path, id: u64) -> crate::Result<()> {
    persist_version(
        folder,
        &Version::new(id, crate::TreeType::Standard),
        "default",
        fs,
        Arc::default(),
        None,
        SyncMode::Normal,
    )
}

/// A temporary, a snapshot and a log of the id being installed, left by a
/// failed attempt (an empty log), are replaced; a log holding edits is a live
/// generation's and is refused, never removed.
#[test]
fn an_install_replaces_a_failed_attempt_and_refuses_a_live_generation() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/tree");
    fs.create_dir_all(folder)?;

    put(&fs, &folder.join("v8.tmp"), b"partial")?;
    put(&fs, &folder.join("v8"), b"partial")?;
    put(&fs, &folder.join("edits-8"), b"")?;
    persist(&fs, folder, 8)?;
    assert_eq!(installed_snapshot(folder, &fs)?, Some(8));
    assert!(
        !fs.exists(&folder.join("v8.tmp"))?,
        "the temporary is renamed into place"
    );
    crate::version::recovery::open_snapshot(folder, 8, &fs, None)?;

    put(&fs, &folder.join("edits-8"), b"an edit")?;
    assert!(
        persist(&fs, folder, 8).is_err(),
        "a snapshot whose log holds edits is live"
    );
    assert!(
        fs.metadata(&folder.join("v8")).is_ok(),
        "the live snapshot stays"
    );
    Ok(())
}

/// An install whose directory sync fails is withdrawn: a withdrawal made
/// durable reports the plain failure, the previous generation installed; one
/// that cannot be (its removal or its sync fails) reports the outcome
/// unknown, since a power loss may still keep the rename.
#[test]
fn a_withdrawal_that_is_not_durable_leaves_the_outcome_unknown() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let folder = Path::new("/tree");
    let cases = [
        // The install's sync fails, the withdrawal's succeeds.
        (
            vec![FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).once()],
            false,
        ),
        // Both syncs fail.
        (
            vec![FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).times(2)],
            true,
        ),
        // The install's sync fails and the snapshot cannot be removed; the
        // first removal is that of a leftover temporary.
        (
            vec![
                FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).once(),
                FaultRule::new(FaultOp::RemoveFile, Fault::Error(ErrorKind::Other))
                    .on_path("/tree/v8")
                    .skip(1)
                    .once(),
            ],
            true,
        ),
    ];
    for (rules, unknown) in cases {
        let fs = FaultFs::new(MemFs::new());
        fs.create_dir_all(folder)?;
        for rule in rules {
            fs.injector().arm(rule);
        }
        let result = persist_version(
            folder,
            &Version::new(8, crate::TreeType::Standard),
            "default",
            &fs,
            Arc::default(),
            None,
            SyncMode::Normal,
        );
        assert!(
            result.is_err(),
            "the armed directory sync fails the install"
        );
        assert_eq!(
            matches!(result, Err(crate::Error::InstallOutcomeUnknown(_))),
            unknown,
            "{result:?}"
        );
    }
    Ok(())
}
