use super::*;
use std::io::{Read, Write};
use std::sync::Arc;
use test_log::test;

#[test]
fn create_read_write() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/data"))?;

    let path = Path::new("/data/test.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hello world")?;
    drop(file);

    let opts = FsOpenOptions::new().read(true);
    let mut file = fs.open(path, &opts)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hello world");

    Ok(())
}

#[test]
fn punch_hole_zeroes_range_keeps_length_and_reclaims_space() -> io::Result<()> {
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(&[0xAB; 600])?;
    drop(file);

    assert!(
        fs.capabilities(path).punch_hole,
        "MemFs advertises punch-hole"
    );
    assert_eq!(fs.available_space(path)?, 400, "1000 capacity − 600 stored");

    // Punch [100, 300): 200 bytes freed.
    fs.punch_hole(path, 100, 200)?;

    let mut buf = Vec::new();
    fs.open(path, &FsOpenOptions::new().read(true))?
        .read_to_end(&mut buf)?;
    assert_eq!(buf.len(), 600, "logical length unchanged by the hole");
    assert!(
        buf.iter().take(100).all(|&b| b == 0xAB),
        "data before the hole is intact"
    );
    assert!(
        buf.iter().skip(100).take(200).all(|&b| b == 0),
        "the hole reads back as zeros"
    );
    assert!(
        buf.iter().skip(300).all(|&b| b == 0xAB),
        "data after the hole is intact"
    );
    assert_eq!(
        fs.available_space(path)?,
        600,
        "1000 capacity − (600 − 200 punched) stored"
    );
    Ok(())
}

#[test]
fn punch_hole_clamps_past_eof_to_a_noop() -> io::Result<()> {
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(&[0xCD; 100])?;
    drop(file);

    // Wholly past EOF → nothing freed.
    fs.punch_hole(path, 200, 50)?;
    assert_eq!(fs.available_space(path)?, 900, "no reclaim past EOF");
    // Straddling EOF → only the in-file portion is freed.
    fs.punch_hole(path, 80, 100)?;
    assert_eq!(
        fs.available_space(path)?,
        920,
        "only [80,100) (20 bytes) freed"
    );
    Ok(())
}

#[test]
fn punch_hole_on_missing_file_is_not_found() {
    let fs = MemFs::new();
    let err = fs.punch_hole(Path::new("/nope"), 0, 10).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn overlapping_prefix_punches_do_not_double_count() -> io::Result<()> {
    // The tight-space reclaim punches strictly-advancing prefixes of the same
    // file. Per-file range merging must count the UNION, not the sum, or
    // available_space would over-report free space under an impossible disk.
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(&[0xAB; 600])?;

    fs.punch_hole(path, 0, 300)?;
    fs.punch_hole(path, 0, 500)?; // overlaps the first punch
    assert_eq!(
        fs.punched_bytes(),
        500,
        "overlapping prefix punches count the union (500), not the sum (800)",
    );
    assert_eq!(
        fs.available_space(path)?,
        900,
        "1000 capacity − (600 − 500 punched) stored",
    );
    Ok(())
}

#[test]
fn removing_a_punched_file_releases_its_reclaim_accounting() -> io::Result<()> {
    // A global punched counter would keep subtracting a removed file's freed
    // bytes, letting available_space report more than the disk can hold.
    // Per-file tracking must drop the accounting on remove.
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(&[0xAB; 400])?;
    fs.punch_hole(path, 0, 400)?;
    assert_eq!(
        fs.available_space(path)?,
        1000,
        "1000 − (400 − 400 punched)"
    );

    fs.remove_file(path)?;
    // The free-space accounting must drop the removed file's reclaim: a global
    // counter would keep subtracting it and report MORE than the full disk.
    assert_eq!(
        fs.available_space(path)?,
        1000,
        "the whole disk is free again after removal, not over-reported",
    );
    Ok(())
}

#[test]
fn truncating_a_punched_file_on_reopen_drops_stale_ranges() -> io::Result<()> {
    // After a punched file is truncated and rewritten larger, the old punched
    // ranges must not resurrect and over-free the freshly written bytes.
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(&[0xAB; 400])?;
    fs.punch_hole(path, 0, 400)?;
    assert_eq!(
        fs.available_space(path)?,
        1000,
        "1000 − (400 − 400 punched)"
    );

    // Truncate-on-open clears the data; rewrite 400 fresh bytes.
    fs.open(path, &FsOpenOptions::new().write(true).truncate(true))?
        .write_all(&[0xCD; 400])?;
    // The stale punched ranges must not resurrect and over-free the fresh
    // bytes: free space reflects only the 400 newly written.
    assert_eq!(
        fs.available_space(path)?,
        600,
        "1000 capacity − 400 freshly written (no phantom reclaim)",
    );
    Ok(())
}

#[test]
fn overwriting_punched_bytes_reclaims_the_space_accounting() -> io::Result<()> {
    // Re-opening a punched file for write (no truncate) and overwriting the
    // hole re-materializes those bytes, so they must stop counting as freed.
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(&[0xAB; 400])?;
    fs.punch_hole(path, 0, 400)?;
    assert_eq!(
        fs.available_space(path)?,
        1000,
        "1000 − (400 − 400 punched)"
    );

    // Overwrite [0,400) in place (no truncate).
    fs.open(path, &FsOpenOptions::new().write(true))?
        .write_all(&[0xCD; 400])?;
    assert_eq!(
        fs.available_space(path)?,
        600,
        "the rewritten hole is no longer free (1000 − 400)",
    );
    Ok(())
}

#[test]
fn shrinking_then_growing_a_punched_file_does_not_resurrect_reclaim() -> io::Result<()> {
    // `set_len` shrink must permanently drop past-end punched ranges so a
    // later grow cannot re-count them as freed.
    let fs = MemFs::with_capacity(1000);
    let path = Path::new("/f");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(&[0xAB; 400])?;
    fs.punch_hole(path, 200, 200)?; // free [200,400)
    assert_eq!(fs.available_space(path)?, 800, "1000 − (400 − 200)");

    // Shrink below the hole, then grow well past it.
    let file = fs.open(path, &FsOpenOptions::new().write(true))?;
    file.set_len(100)?;
    file.set_len(500)?;
    assert_eq!(
        fs.available_space(path)?,
        500,
        "the dropped hole must not resurrect on grow (1000 − 500)",
    );
    Ok(())
}

#[test]
fn writing_through_a_pre_rename_handle_invalidates_reclaim() -> io::Result<()> {
    // A handle opened before a rename keeps the OLD path but writes to the
    // SAME backing buffer (now under the new path). Punched-range
    // invalidation must follow the backing file, not the stale path, or the
    // rewritten bytes keep counting as reclaimed.
    let fs = MemFs::with_capacity(1000);
    fs.open(
        Path::new("/a"),
        &FsOpenOptions::new().write(true).create(true),
    )?
    .write_all(&[0xAB; 400])?;
    // A writable handle captured BEFORE the punch + rename.
    let mut pre_rename = fs.open(Path::new("/a"), &FsOpenOptions::new().write(true))?;

    fs.punch_hole(Path::new("/a"), 0, 400)?;
    fs.rename(Path::new("/a"), Path::new("/b"))?;
    assert_eq!(
        fs.available_space(Path::new("/b"))?,
        1000,
        "1000 − (400 − 400)"
    );

    // Rewrite the hole through the stale handle (its buffer is now `/b`).
    pre_rename.write_all(&[0xCD; 400])?;
    assert_eq!(
        fs.available_space(Path::new("/b"))?,
        600,
        "the rewritten bytes must stop counting as reclaimed (1000 − 400)",
    );
    Ok(())
}

#[test]
fn directory_operations() -> io::Result<()> {
    let fs = MemFs::new();
    let nested = PathBuf::from("/a/b/c");
    fs.create_dir_all(&nested)?;
    assert!(fs.exists(&nested)?);
    assert!(fs.exists(Path::new("/a/b"))?);

    let file_path = nested.join("data.bin");
    let opts = FsOpenOptions::new().write(true).create_new(true);
    let mut file = fs.open(&file_path, &opts)?;
    file.write_all(b"data")?;
    drop(file);

    let entries = fs.read_dir(&nested)?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].file_name, "data.bin");
    assert!(!entries[0].is_dir);

    let meta = fs.metadata(&file_path)?;
    assert!(meta.is_file);
    assert!(!meta.is_dir);
    assert_eq!(meta.len, 4);

    fs.remove_file(&file_path)?;
    assert!(!fs.exists(&file_path)?);

    fs.remove_dir_all(Path::new("/a"))?;
    assert!(!fs.exists(Path::new("/a"))?);
    assert!(!fs.exists(&nested)?);

    Ok(())
}

#[test]
fn rename_file() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let src = Path::new("/dir/src.txt");
    let dst = Path::new("/dir/dst.txt");

    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(src, &opts)?;
    file.write_all(b"content")?;
    drop(file);

    fs.rename(src, dst)?;
    assert!(!fs.exists(src)?);
    assert!(fs.exists(dst)?);

    Ok(())
}

#[test]
fn rename_atomically_replaces_existing_destination() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let src = Path::new("/dir/new.txt");
    let dst = Path::new("/dir/existing.txt");

    // Create destination with old content
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(dst, &opts)?;
    file.write_all(b"old")?;
    drop(file);

    // Create source with new content
    let mut file = fs.open(src, &opts)?;
    file.write_all(b"new")?;
    drop(file);

    // Rename should atomically replace destination
    fs.rename(src, dst)?;
    assert!(!fs.exists(src)?);

    let mut file = fs.open(dst, &FsOpenOptions::new().read(true))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "new");

    Ok(())
}

#[test]
fn sync_directory_is_noop() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    fs.sync_directory(Path::new("/dir"))?;
    Ok(())
}

#[test]
fn file_metadata_and_set_len() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/meta.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"12345")?;

    let meta = file.metadata()?;
    assert!(meta.is_file);
    assert_eq!(meta.len, 5);

    file.set_len(3)?;
    let meta = file.metadata()?;
    assert_eq!(meta.len, 3);

    Ok(())
}

#[test]
fn read_at_positional() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/pread.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hello world")?;

    let mut buf = [0u8; 5];
    let n = file.read_at(&mut buf, 6)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"world");

    let n = file.read_at(&mut buf, 0)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");

    // Past EOF
    let n = file.read_at(&mut buf, 100)?;
    assert_eq!(n, 0);

    Ok(())
}

#[test]
fn lock_exclusive_is_noop() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/lock");
    let opts = FsOpenOptions::new().write(true).create(true);
    let file = fs.open(path, &opts)?;
    file.lock_exclusive()?;
    Ok(())
}

#[test]
fn open_create_new_fails_on_existing() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/file");
    let opts = FsOpenOptions::new().write(true).create_new(true);
    fs.open(path, &opts)?;

    let err = fs.open(path, &opts).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    Ok(())
}

#[test]
fn open_nonexistent_without_create_fails() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/missing");
    let opts = FsOpenOptions::new().read(true);
    let err = fs.open(path, &opts).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn open_fails_when_parent_missing() -> io::Result<()> {
    let fs = MemFs::new();
    let path = Path::new("/no/such/dir/file");
    let opts = FsOpenOptions::new().write(true).create(true);
    let err = fs.open(path, &opts).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn truncate_on_open() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/trunc.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hello world")?;
    drop(file);

    let opts = FsOpenOptions::new().write(true).truncate(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hi")?;
    drop(file);

    let meta = fs.metadata(path)?;
    assert_eq!(meta.len, 2);
    Ok(())
}

#[test]
fn append_mode() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/append.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hello")?;
    drop(file);

    let opts = FsOpenOptions::new().write(true).append(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b" world")?;
    drop(file);

    let opts = FsOpenOptions::new().read(true);
    let mut file = fs.open(path, &opts)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hello world");
    Ok(())
}

#[test]
fn read_append_cursor_starts_at_zero() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/rw_append.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"existing")?;
    drop(file);

    // Open with read + append - cursor should start at 0 for reads,
    // but writes go to EOF.
    let opts = FsOpenOptions::new().read(true).append(true);
    let mut file = fs.open(path, &opts)?;

    // Read should return existing content from offset 0.
    let mut buf = [0u8; 8];
    let n = file.read(&mut buf)?;
    assert_eq!(n, 8);
    assert_eq!(&buf, b"existing");

    // Write appends to EOF.
    file.write_all(b"+new")?;
    drop(file);

    // Verify full content.
    let opts = FsOpenOptions::new().read(true);
    let mut file = fs.open(path, &opts)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "existing+new");

    Ok(())
}

#[test]
fn seek_and_overwrite() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/seek.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"hello world")?;

    file.seek(std::io::SeekFrom::Start(6))?;
    file.write_all(b"rust!")?;

    file.seek(std::io::SeekFrom::Start(0))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hello rust!");

    Ok(())
}

#[test]
fn object_safety() -> io::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let bogus = Path::new("/nonexistent");
    assert!(!fs.exists(bogus)?);
    Ok(())
}

#[test]
fn metadata_directory() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/mydir"))?;
    let meta = fs.metadata(Path::new("/mydir"))?;
    assert!(meta.is_dir);
    assert!(!meta.is_file);
    Ok(())
}

#[test]
fn read_dir_with_subdirectory() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/root/subdir"))?;

    let file_path = Path::new("/root/file.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(file_path, &opts)?;

    let mut entries = fs.read_dir(Path::new("/root"))?;
    entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].file_name, "file.txt");
    assert!(!entries[0].is_dir);
    assert_eq!(entries[1].file_name, "subdir");
    assert!(entries[1].is_dir);
    Ok(())
}

#[test]
fn remove_file_nonexistent_fails() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs.remove_file(Path::new("/missing")).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn rename_nonexistent_fails() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs
        .rename(Path::new("/missing"), Path::new("/dst"))
        .err()
        .unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn read_dir_nonexistent_fails() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs.read_dir(Path::new("/missing")).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn metadata_nonexistent_fails() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs.metadata(Path::new("/missing")).err().unwrap();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn sync_data_is_noop() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let path = Path::new("/dir/file");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(path, &opts)?;
    file.write_all(b"data")?;
    file.sync_data()?;
    Ok(())
}

#[test]
fn clones_share_state() -> io::Result<()> {
    let fs1 = MemFs::new();
    let fs2 = fs1.clone();

    fs1.create_dir_all(Path::new("/shared"))?;
    let path = Path::new("/shared/file.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs1.open(path, &opts)?;
    file.write_all(b"shared data")?;
    drop(file);

    assert!(fs2.exists(path)?);
    let meta = fs2.metadata(path)?;
    assert_eq!(meta.len, 11);
    Ok(())
}

// ── Wrong-type error-path tests ─────────────────────────────────────

#[test]
fn read_dir_on_file_returns_not_a_directory() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;

    let err = fs.read_dir(Path::new("/dir/file")).unwrap_err();
    // Must NOT be NotFound - the path exists but is a file.
    assert_ne!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn remove_file_on_dir_returns_error() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/somedir"))?;

    let err = fs.remove_file(Path::new("/somedir")).unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn sync_directory_on_file_returns_not_a_directory() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;

    let err = fs.sync_directory(Path::new("/dir/file")).unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn open_with_parent_as_file_returns_error() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;

    // Try to create a file whose "parent" is actually a file.
    let err = fs
        .open(Path::new("/dir/file/child"), &opts)
        .map(|_| ())
        .unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn rename_directory_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/src_dir"))?;
    fs.create_dir_all(Path::new("/dst_parent"))?;

    let err = fs
        .rename(Path::new("/src_dir"), Path::new("/dst_parent/moved"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn rename_onto_directory_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;
    fs.create_dir_all(Path::new("/dir/dst_dir"))?;

    let err = fs
        .rename(Path::new("/dir/file"), Path::new("/dir/dst_dir"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn rename_with_file_as_dest_parent_returns_error() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/src"), &opts)?;
    fs.open(Path::new("/dir/blocker"), &opts)?;

    // /dir/blocker is a file, not a directory - cannot be parent of dst.
    let err = fs
        .rename(Path::new("/dir/src"), Path::new("/dir/blocker/child"))
        .unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn remove_dir_all_on_file_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;

    let err = fs.remove_dir_all(Path::new("/dir/file")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn set_len_without_write_access_returns_error() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/file.bin");
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(b"data")?;
    drop(file);

    let file = fs.open(path, &FsOpenOptions::new().read(true))?;
    assert!(file.set_len(1).is_err());
    Ok(())
}

#[test]
fn read_at_without_read_access_returns_error() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let path = Path::new("/dir/file.bin");
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(b"data")?;

    let mut buf = [0u8; 1];
    assert!(file.read_at(&mut buf, 0).is_err());
    Ok(())
}

#[test]
fn open_empty_path_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs
        .open(Path::new(""), &FsOpenOptions::new().read(true))
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn create_dir_all_empty_path_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    let err = fs.create_dir_all(Path::new("")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn rename_empty_path_returns_invalid_input() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/file"), &opts)?;

    let err = fs.rename(Path::new(""), Path::new("/dir/dst")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    let err = fs
        .rename(Path::new("/dir/file"), Path::new(""))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn hard_link_creates_independent_copy() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let src = Path::new("/dir/src.bin");
    let dst = Path::new("/dir/dst.bin");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(src, &opts)?;
    file.write_all(b"checkpoint")?;
    drop(file);

    fs.hard_link(src, dst)?;

    // Both exist and contain the same bytes.
    let opts = FsOpenOptions::new().read(true);
    let mut buf = String::new();
    fs.open(src, &opts)?.read_to_string(&mut buf)?;
    assert_eq!(buf, "checkpoint");
    let mut buf = String::new();
    fs.open(dst, &opts)?.read_to_string(&mut buf)?;
    assert_eq!(buf, "checkpoint");

    // Critical invariant: `MemFs::hard_link` returns an *independent*
    // copy (no `Arc<Mutex<Vec<u8>>>` aliasing). Mutate the source and
    // verify the destination is unaffected - if the test only relied
    // on `remove_file` it would pass even with an aliased buffer.
    let mut writer = fs.open(src, &FsOpenOptions::new().write(true).truncate(true))?;
    writer.write_all(b"mutated")?;
    drop(writer);

    let mut after = String::new();
    fs.open(dst, &FsOpenOptions::new().read(true))?
        .read_to_string(&mut after)?;
    assert_eq!(
        after, "checkpoint",
        "dst must not see writes to src - buffers must be independent",
    );

    // Removing the source leaves the destination intact.
    fs.remove_file(src)?;
    assert!(!fs.exists(src)?);
    assert!(fs.exists(dst)?);
    Ok(())
}

#[test]
fn fs_capabilities_default_reports_no_guarantees() {
    // The conservative default is load-bearing: any backend that does not
    // override capabilities() must be treated as offering nothing, so an
    // unknown FS never skips a checksum or disables `CoW` by accident.
    let caps = FsCapabilities::default();
    assert!(!caps.per_block_integrity_on_read);
    assert!(!caps.background_scrub);
    assert!(!caps.copy_on_write);
    assert!(!caps.reflink);
    assert!(!caps.native_snapshot);
}

#[test]
fn memfs_capabilities_advertise_only_punch_hole() {
    // RAM has no FS-level integrity / `CoW` / reflink, so those stay false;
    // only `punch_hole` is set, since `MemFs::punch_hole` simulates in-place
    // extent reclaim for tight-space compaction tests.
    assert_eq!(
        MemFs::new().capabilities(Path::new("/dir/sst.bin")),
        FsCapabilities {
            punch_hole: true,
            ..FsCapabilities::default()
        }
    );
}

#[test]
fn try_disable_cow_without_cow_support_is_noop() {
    // MemFs reports copy_on_write=false, so the default no-op path applies:
    // the call succeeds and changes nothing.
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir")).unwrap();
    let path = Path::new("/dir/sst.bin");
    fs.open(path, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    assert!(
        fs.try_disable_cow(path).is_ok(),
        "no-op must succeed on a non-CoW backend"
    );
}

#[test]
fn reflink_file_without_backend_support_copies_independently() -> io::Result<()> {
    // No backend reflink support → default streamed-copy fallback. The
    // clone must be byte-identical AND an independent file (writing the
    // source afterwards must not change the clone).
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;
    let src = Path::new("/dir/src.bin");
    let dst = Path::new("/dir/clone.bin");

    let mut f = fs.open(src, &FsOpenOptions::new().write(true).create(true))?;
    f.write_all(b"original-contents")?;
    drop(f);

    fs.reflink_file(src, dst)?;

    let mut buf = String::new();
    fs.open(dst, &FsOpenOptions::new().read(true))?
        .read_to_string(&mut buf)?;
    assert_eq!(buf, "original-contents");

    // Independence: mutate src, clone must be unaffected.
    let mut w = fs.open(src, &FsOpenOptions::new().write(true).truncate(true))?;
    w.write_all(b"changed")?;
    drop(w);

    let mut after = String::new();
    fs.open(dst, &FsOpenOptions::new().read(true))?
        .read_to_string(&mut after)?;
    assert_eq!(
        after, "original-contents",
        "reflink clone must be independent"
    );

    Ok(())
}

#[test]
fn reflink_file_rejects_existing_destination() {
    // Default fallback opens dst with create_new, so an existing target is
    // an error (no silent overwrite of a checkpoint file).
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir")).unwrap();
    let src = Path::new("/dir/src.bin");
    let dst = Path::new("/dir/dst.bin");
    for p in [src, dst] {
        fs.open(p, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
    }
    let err = fs.reflink_file(src, dst).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
}

#[test]
fn hard_link_rejects_existing_destination() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(Path::new("/dir/a"), &opts)?;
    fs.open(Path::new("/dir/b"), &opts)?;

    let err = fs
        .hard_link(Path::new("/dir/a"), Path::new("/dir/b"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    Ok(())
}

#[test]
fn hard_link_rejects_missing_source() -> io::Result<()> {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/dir"))?;

    let err = fs
        .hard_link(Path::new("/dir/missing"), Path::new("/dir/dst"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}
