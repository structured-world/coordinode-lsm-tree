use super::clear_unnamed_snapshot;
use crate::file::CURRENT_VERSION_FILE;
use crate::fs::{Fs, FsOpenOptions, MemFs};
use crate::path::Path;

/// Writes `bytes` to `path`, creating it.
fn put(fs: &MemFs, path: &Path, bytes: &[u8]) -> crate::Result<()> {
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(bytes)?;
    Ok(())
}

/// `CURRENT` naming version `id`: the id, a checksum and its type tag.
fn current_naming(id: u64) -> Vec<u8> {
    let mut bytes = id.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0; 16]);
    bytes.push(0);
    bytes
}

/// A snapshot `CURRENT` does not name is a failed attempt's leftover and goes,
/// so the rotation can create it; one `CURRENT` names is the live manifest and
/// must be refused, never removed.
#[test]
fn clear_unnamed_snapshot_removes_only_a_snapshot_current_does_not_name() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/tree");
    fs.create_dir_all(folder)?;
    put(&fs, &folder.join(CURRENT_VERSION_FILE), &current_naming(7))?;

    let live = folder.join("v7");
    put(&fs, &live, b"live")?;
    let leftover = folder.join("v8");
    put(&fs, &leftover, b"partial")?;

    assert!(
        clear_unnamed_snapshot(folder, &live, 7, &fs).is_err(),
        "the snapshot CURRENT names must be refused"
    );
    assert!(fs.metadata(&live).is_ok(), "the live snapshot must stay");

    clear_unnamed_snapshot(folder, &leftover, 8, &fs)?;
    assert!(
        fs.metadata(&leftover).is_err(),
        "the leftover must be removed"
    );

    // Nothing there: nothing to do.
    clear_unnamed_snapshot(folder, &folder.join("v9"), 9, &fs)?;
    Ok(())
}
