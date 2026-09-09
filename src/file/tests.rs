use super::*;
use crate::fs::StdFs;
use std::fs::File;
use std::io::Write;
use test_log::test;

#[test]
fn dict_dir_entry_classifies_the_shapes_the_engine_owns() {
    assert_eq!(DictDirEntry::classify("7"), DictDirEntry::Dict(7));
    assert_eq!(DictDirEntry::classify("0"), DictDirEntry::Dict(0));
    assert_eq!(
        DictDirEntry::classify(&u32::MAX.to_string()),
        DictDirEntry::Dict(u32::MAX),
    );
    assert_eq!(DictDirEntry::classify("7.tmp"), DictDirEntry::Tmp(7));
}

#[test]
fn dict_dir_entry_refuses_every_name_it_does_not_own() {
    // A name that merely LOOKS owned is foreign, so an operator's file beside
    // the dictionaries is never swept: the same exact-shape rule the table and
    // blob grammars enforce.
    for name in [
        "notes.tmp",    // suffix without an id
        "7.tmp.backup", // owned shape with something appended
        "7.dict",       // unknown suffix
        "-1",           // not a u32
        "4294967296",   // one past u32::MAX
        "7 ",           // trailing space
        "",             // empty
        "07x",          // trailing garbage
        // PARSEABLE but not canonical. The engine writes `id.to_string()` and
        // nothing else, so these are an operator's names: sweeping `01.tmp`
        // would break the promise never to touch a foreign file, and reading
        // `01` as id 1 would open the entirely different file `dicts/1`.
        "01",
        "00",
        "+1",
        "01.tmp",
        "+7.tmp",
    ] {
        assert_eq!(
            DictDirEntry::classify(name),
            DictDirEntry::Foreign,
            "{name} must not be classified as engine state",
        );
    }
}

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
