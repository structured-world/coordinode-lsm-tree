use super::sync_on_ring;
use crate::fs::{Fs, StdFs, SyncMode, SyncTarget, WriteOrigin};
use std::io::Write;

/// A batch of file and folder syncs goes through one ring and reports every
/// one done: the path a synced flush takes on Linux.
#[test]
fn a_batch_of_files_and_folders_syncs_through_one_ring() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut paths = Vec::new();
    for name in ["a", "b", "c"] {
        let path = dir.path().join(name);
        std::fs::File::create(&path)?.write_all(name.as_bytes())?;
        paths.push(path);
    }
    let mut targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    targets.push(SyncTarget::Folder {
        path: dir.path(),
        mode: SyncMode::Normal,
    });
    let Some(result) = sync_on_ring(&targets) else {
        // A kernel that refuses io_uring has nothing to test here; the batch
        // is synced on threads instead (see the next test).
        return Ok(());
    };
    result?;
    // The same batch through the backend, whichever way it syncs.
    StdFs.sync_many(&targets)?;
    Ok(())
}

/// A target that cannot be opened fails the batch with its own error, before
/// anything is submitted: a missing file is reported missing, not synced.
#[test]
fn a_missing_file_fails_the_batch_with_not_found() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let present = dir.path().join("present");
    std::fs::File::create(&present)?;
    let missing = dir.path().join("missing");
    let targets = [
        SyncTarget::FileData {
            path: &present,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        },
        SyncTarget::FileData {
            path: &missing,
            origin: WriteOrigin::FlushBlob,
            mode: SyncMode::Normal,
        },
    ];
    let Some(result) = sync_on_ring(&targets) else {
        return Ok(());
    };
    let Err(e) = result else {
        panic!("a missing file was reported synced");
    };
    assert_eq!(e.kind(), crate::io::ErrorKind::NotFound);
    Ok(())
}

/// A folder target that names a file is refused, as
/// `StdFs::sync_directory_with` refuses it: the ring path does not sync a
/// file in place of the folder a caller meant to make durable.
#[test]
fn a_folder_target_naming_a_file_is_refused() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("file");
    std::fs::File::create(&file)?;
    let targets = [
        SyncTarget::Folder {
            path: &file,
            mode: SyncMode::Normal,
        },
        SyncTarget::Folder {
            path: dir.path(),
            mode: SyncMode::Normal,
        },
    ];
    let Some(result) = sync_on_ring(&targets) else {
        return Ok(());
    };
    let Err(e) = result else {
        panic!("a file was synced as a folder");
    };
    assert_eq!(e.kind(), crate::io::ErrorKind::InvalidInput);
    Ok(())
}
