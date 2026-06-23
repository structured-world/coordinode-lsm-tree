use super::*;
use crate::fs::StdFs;
use std::fs::File;
use std::io::Write;
use test_log::test;

#[test]
fn read_exact_short_read_returns_error() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("short.bin");
    {
        let mut f = File::create(&path)?;
        f.write_all(b"hello")?; // 5 bytes
    }

    let file = File::open(&path)?;
    // Request 10 bytes from a 5-byte file → short read → UnexpectedEof
    let err = read_exact(&file, 0, 10).unwrap_err();
    assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof);

    Ok(())
}

#[test]
fn atomic_rewrite() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let path = dir.path().join("test.txt");
    {
        let mut file = File::create(&path)?;
        write!(file, "asdasdasdasdasd")?;
    }

    rewrite_atomic(&path, b"newcontent", &StdFs, SyncMode::Normal)?;

    let content = std::fs::read_to_string(&path)?;
    assert_eq!("newcontent", content);

    Ok(())
}

/// Verifies that `StdFs::rename` atomically replaces an existing
/// destination file — the contract required by `rewrite_atomic`.
#[test]
fn std_fs_rename_replaces_existing_file() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions};

    let dir = tempfile::tempdir()?;
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");

    // Create both files via Fs trait.
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut f = StdFs.open(&src, &opts)?;
    f.write_all(b"new")?;
    drop(f);

    let mut f = StdFs.open(&dst, &opts)?;
    f.write_all(b"old")?;
    drop(f);

    StdFs.rename(&src, &dst)?;

    // dst now has src content, src is gone.
    let content = std::fs::read_to_string(&dst)?;
    assert_eq!("new", content);
    assert!(!src.exists());

    Ok(())
}
