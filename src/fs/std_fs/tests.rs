use super::*;
use std::io::{Read, Write};
use std::sync::Arc;
use test_log::test;

/// Linux: `fallocate(PUNCH_HOLE)` frees a mid-file range and reads it back
/// as zeros, leaving the logical length unchanged. Skips cleanly on a mount
/// that does not advertise the capability (e.g. overlayfs in CI), so the
/// test never fails on an unsupported filesystem.
#[cfg(target_os = "linux")]
#[test]
fn std_fs_punch_hole_zeroes_range_on_linux() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("f");
    std::fs::write(&path, vec![0xABu8; 16 * 1024])?;

    if !StdFs.capabilities(&path).punch_hole {
        return Ok(()); // mount without punch-hole support → nothing to assert
    }

    // Block-aligned range so the extent is actually deallocated.
    StdFs.punch_hole(&path, 4096, 4096)?;

    let data = std::fs::read(&path)?;
    assert_eq!(data.len(), 16 * 1024, "logical length unchanged");
    assert!(
        data.iter().skip(4096).take(4096).all(|&b| b == 0),
        "the punched range reads back as zeros"
    );
    assert!(
        data.iter().take(4096).all(|&b| b == 0xAB),
        "data before the hole is intact"
    );
    Ok(())
}

/// Off Linux, `StdFs` keeps the trait default: hole punching is unsupported
/// and the capability is not advertised, so tight-space compaction never
/// engages on those targets.
#[cfg(not(target_os = "linux"))]
#[test]
fn std_fs_punch_hole_is_unsupported_off_linux() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("f");
    std::fs::write(&path, vec![0u8; 64])?;

    assert!(
        !StdFs.capabilities(&path).punch_hole,
        "punch-hole capability is not advertised off Linux"
    );
    let err = StdFs.punch_hole(&path, 0, 16).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    Ok(())
}

#[test]
fn std_fs_create_read_write() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    // Create and write
    let path = dir.path().join("test.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    file.sync_all()?;
    drop(file);

    // Read back
    let opts = FsOpenOptions::new().read(true);
    let mut file = fs.open(&path, &opts)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hello world");

    Ok(())
}

#[cfg(unix)]
#[test]
fn std_fs_volume_id_is_shared_within_a_mount_and_none_for_missing_path() -> io::Result<()> {
    // `volume_id` is the `st_dev` of the mount: two directories under one
    // tempdir share it (one free-space pool), so the space gate combines
    // their budgets. A path that cannot be stat-ed yields `None`
    // (independence unproven → callers combine conservatively).
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    fs.create_dir_all(&a)?;
    fs.create_dir_all(&b)?;
    let id_a = fs.volume_id(&a);
    assert!(id_a.is_some(), "a real path reports its device id");
    assert_eq!(id_a, fs.volume_id(&b), "same mount → same volume id");
    assert_eq!(
        fs.volume_id(&dir.path().join("does-not-exist")),
        None,
        "an un-stat-able path is unproven, not falsely independent"
    );
    Ok(())
}

#[test]
fn std_fs_sync_with_both_modes_persists() -> io::Result<()> {
    // Both durability modes must succeed and leave the bytes readable.
    // On macOS this exercises the plain-`fsync` (Normal) and
    // `F_FULLFSYNC` (Full) branches; elsewhere both are plain `fsync`.
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    for (name, mode) in [("normal", SyncMode::Normal), ("full", SyncMode::Full)] {
        let path = dir.path().join(name);
        let mut file = fs.open(&path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(b"durable")?;
        file.sync_data_with(mode)?;
        file.sync_all_with(mode)?;
        drop(file);
        // Directory sync at both modes must also succeed.
        fs.sync_directory_with(dir.path(), mode)?;

        let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "durable", "{name} mode lost data");
    }
    Ok(())
}

#[test]
fn std_fs_directory_operations() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let nested = dir.path().join("a").join("b").join("c");
    fs.create_dir_all(&nested)?;
    assert!(fs.exists(&nested)?);

    // Create a file inside
    let file_path = nested.join("data.bin");
    let opts = FsOpenOptions::new().write(true).create_new(true);
    let mut file = fs.open(&file_path, &opts)?;
    file.write_all(b"data")?;
    drop(file);

    // read_dir
    let entries = fs.read_dir(&nested)?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].file_name, "data.bin");
    assert!(!entries[0].is_dir);

    // metadata
    let meta = fs.metadata(&file_path)?;
    assert!(meta.is_file);
    assert!(!meta.is_dir);
    assert_eq!(meta.len, 4);

    // remove_file
    fs.remove_file(&file_path)?;
    assert!(!fs.exists(&file_path)?);

    // remove_dir_all
    let top = dir.path().join("a");
    fs.remove_dir_all(&top)?;
    assert!(!fs.exists(&top)?);

    Ok(())
}

#[test]
fn std_fs_rename() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");

    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&src, &opts)?;
    file.write_all(b"content")?;
    drop(file);

    fs.rename(&src, &dst)?;
    assert!(!fs.exists(&src)?);
    assert!(fs.exists(&dst)?);

    Ok(())
}

#[test]
fn std_fs_sync_directory() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    // Should not error on valid directories
    fs.sync_directory(dir.path())?;
    Ok(())
}

#[test]
fn fs_file_metadata() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("meta.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"12345")?;

    let meta = file.metadata()?;
    assert!(meta.is_file);
    assert_eq!(meta.len, 5);

    Ok(())
}

#[test]
fn fs_file_set_len() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("truncate.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    file.set_len(5)?;

    let meta = file.metadata()?;
    assert_eq!(meta.len, 5);

    Ok(())
}

#[test]
fn fs_file_read_many_fills_every_region() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("read_many.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    // 0..=255 so each region's bytes are self-identifying by offset.
    let data: Vec<u8> = (0..=255u8).collect();
    file.write_all(&data)?;

    // Three disjoint regions in non-file order, covering the start and a
    // single-byte read, must each be filled completely.
    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 8];
    let mut b2 = [0u8; 1];
    let mut regions: Vec<(u64, &mut [u8])> =
        vec![(10, &mut b0[..]), (200, &mut b1[..]), (0, &mut b2[..])];
    file.read_many(&mut regions)?;
    drop(regions);

    assert_eq!(&b0[..], &data[10..14]);
    assert_eq!(&b1[..], &data[200..208]);
    assert_eq!(b2[0], data[0]);

    // The default read_many must agree byte-for-byte with per-region read_at.
    let mut single = [0u8; 8];
    assert_eq!(file.read_at(&mut single, 200)?, 8);
    assert_eq!(single, b1);

    Ok(())
}

#[test]
fn fs_read_blocks_batched_across_files() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut f0 = fs.open(&dir.path().join("a.bin"), &opts)?;
    f0.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    let mut f1 = fs.open(&dir.path().join("b.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    f1.write_all(&rev)?;

    // Reads spanning TWO files in one batched call, out of order, must each be
    // filled from the right file at the right offset.
    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    let mut b2 = [0u8; 2];
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: f0.as_ref(),
                offset: 10,
                buf: &mut b0,
            },
            crate::fs::BlockRead {
                file: f1.as_ref(),
                offset: 20,
                buf: &mut b1,
            },
            crate::fs::BlockRead {
                file: f0.as_ref(),
                offset: 0,
                buf: &mut b2,
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }

    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    assert_eq!(b2, [0, 1]);
    Ok(())
}

#[test]
fn fs_file_read_many_short_read_at_eof_errors() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("short_many.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(&[0u8; 10])?;

    // A region that runs past EOF cannot be filled completely; the default
    // read_many treats the fixed-size short read as UnexpectedEof, not EOF.
    let mut buf = [0u8; 32];
    let mut regions: Vec<(u64, &mut [u8])> = vec![(0, &mut buf[..])];
    let err = file.read_many(&mut regions).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    Ok(())
}

#[test]
fn fs_read_blocks_batched_short_read_at_eof_errors() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&dir.path().join("short_block.bin"), &opts)?;
    file.write_all(&[0u8; 10])?;

    let mut buf = [0u8; 64];
    {
        let mut reqs = vec![crate::fs::BlockRead {
            file: file.as_ref(),
            offset: 0,
            buf: &mut buf,
        }];
        let err = fs.read_blocks_batched(&mut reqs).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
    Ok(())
}

#[test]
fn fs_file_backing_fd_default_is_none() -> io::Result<()> {
    // StdFs handles use the trait's default backing_fd (no shared io_uring ring),
    // so read_blocks_batched reads them serially rather than via a ring.
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let file = fs.open(&dir.path().join("nofd.bin"), &opts)?;
    assert_eq!(file.backing_fd(), None);
    Ok(())
}

#[test]
#[cfg(any(unix, windows))]
fn fs_file_lock_exclusive() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("lockfile");
    let opts = FsOpenOptions::new().write(true).create(true);
    let file = fs.open(&path, &opts)?;
    file.lock_exclusive()?;

    // Verifies flock() syscall succeeds without error. Testing actual
    // lock contention (try_lock from second thread) is out of scope for
    // the Fs trait definition - belongs in integration tests.
    Ok(())
}

#[test]
#[cfg(any(unix, windows))]
fn fs_file_read_at() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("pread.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;

    // read_at at offset 6 should return "world"
    let mut buf = [0u8; 5];
    let n = file.read_at(&mut buf, 6)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"world");

    // read_at at offset 0 should return "hello"
    let n = file.read_at(&mut buf, 0)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");

    Ok(())
}

#[test]
fn fs_open_options_default() {
    let opts = FsOpenOptions::default();
    assert!(!opts.read);
    assert!(!opts.write);
    assert!(!opts.create);
    assert!(!opts.create_new);
    assert!(!opts.truncate);
    assert!(!opts.append);
    assert!(!opts.direct_io);
}

#[test]
fn fs_open_options_builders() {
    let opts = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .create_new(false)
        .truncate(true)
        .append(false)
        .direct_io(true);
    assert!(opts.read);
    assert!(opts.write);
    assert!(opts.create);
    assert!(!opts.create_new);
    assert!(opts.truncate);
    assert!(!opts.append);
    assert!(opts.direct_io);
}

#[test]
fn fs_file_sync_data() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let path = dir.path().join("sync_data.bin");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"data")?;
    file.sync_data()?;

    Ok(())
}

#[test]
fn fs_open_truncate_and_append() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let path = dir.path().join("trunc.txt");

    // Create with content
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    drop(file);

    // Truncate on reopen
    let opts = FsOpenOptions::new().write(true).truncate(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hi")?;
    drop(file);

    let meta = fs.metadata(&path)?;
    assert_eq!(meta.len, 2);

    // Append mode
    let opts = FsOpenOptions::new().write(true).append(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"!")?;
    drop(file);

    let meta = fs.metadata(&path)?;
    assert_eq!(meta.len, 3);

    Ok(())
}

#[test]
fn fs_dir_entry_fields() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    // Create a subdirectory and a file
    let sub = dir.path().join("subdir");
    fs.create_dir_all(&sub)?;
    let file_path = dir.path().join("file.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(&file_path, &opts)?;

    let mut entries = fs.read_dir(dir.path())?;
    entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].file_name, "file.txt");
    assert!(!entries[0].is_dir);
    assert_eq!(entries[1].file_name, "subdir");
    assert!(entries[1].is_dir);

    Ok(())
}

#[test]
fn fs_metadata_directory() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let meta = fs.metadata(dir.path())?;
    assert!(meta.is_dir);
    assert!(!meta.is_file);

    Ok(())
}

// Linux only: macOS (HFS+/APFS) rejects non-UTF-8 filenames at the FS layer.
#[test]
#[cfg(target_os = "linux")]
fn read_dir_rejects_non_utf8_filename() -> io::Result<()> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir()?;
    // Create a file with invalid UTF-8 bytes in its name.
    let bad_name = OsStr::from_bytes(&[0xff, 0xfe]);
    let bad_path = dir.path().join(bad_name);
    if std::fs::write(&bad_path, b"data").is_err() {
        // Filesystem rejected the non-UTF-8 filename (e.g. overlay,
        // container mounts, restrictive mount options) - test
        // precondition cannot be met, skip gracefully.
        return Ok(());
    }

    let fs = StdFs;
    match fs.read_dir(dir.path()) {
        Err(err) => {
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            let msg = err.to_string();
            assert!(
                msg.contains("non-UTF-8 filename"),
                "unexpected error: {msg}"
            );
            assert!(
                msg.contains(&dir.path().display().to_string()),
                "error should include directory path: {msg}",
            );
        }
        Ok(_) => panic!("read_dir should fail on non-UTF-8 filename"),
    }
    Ok(())
}

/// Compile-time assertion: `Fs` is object-safe without specifying
/// associated types - enables simple `Arc<dyn Fs>` for per-level routing.
#[test]
fn object_safety() -> io::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let dir = tempfile::tempdir()?;
    let bogus = dir.path().join("nonexistent");
    assert!(!fs.exists(&bogus)?);
    Ok(())
}

#[test]
fn hard_link_creates_second_path_to_same_inode() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let src = dir.path().join("src.bin");
    let dst = dir.path().join("dst.bin");
    std::fs::write(&src, b"checkpoint payload")?;

    fs.hard_link(&src, &dst)?;

    // Both paths exist and have the same content.
    assert_eq!(std::fs::read(&dst)?, b"checkpoint payload");

    // Mutating the link changes both views (same inode), proving this
    // was a true hard link, not a copy. We only check this on Unix -
    // Windows hard links share content but inode equality is not
    // exposed through std.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let src_meta = std::fs::metadata(&src)?;
        let dst_meta = std::fs::metadata(&dst)?;
        assert_eq!(src_meta.ino(), dst_meta.ino());
        assert_eq!(src_meta.dev(), dst_meta.dev());
        assert_eq!(dst_meta.nlink(), 2);
    }

    // Removing the source leaves the link intact.
    fs.remove_file(&src)?;
    assert_eq!(std::fs::read(&dst)?, b"checkpoint payload");
    Ok(())
}

#[test]
fn hard_link_rejects_existing_destination() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::write(&src, b"src")?;
    std::fs::write(&dst, b"dst")?;

    let err = fs.hard_link(&src, &dst).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    Ok(())
}

#[test]
fn hard_link_rejects_missing_source() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = StdFs;

    let err = fs
        .hard_link(&dir.path().join("missing"), &dir.path().join("dst"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn is_cross_device_detects_exdev_and_kind_variants() {
    // Synthesise a raw EXDEV error - what Linux/macOS/BSDs return when
    // hard_link spans devices. `is_cross_device` must accept it so the
    // fallback copy path kicks in. Gated to Unix because errno 18 has
    // a different meaning on Windows (ERROR_NO_MORE_FILES) and the
    // `#[cfg(unix)]` branch in `is_cross_device` is never compiled on
    // non-Unix targets.
    #[cfg(unix)]
    {
        // Matches the `EXDEV` constant inside `is_cross_device`.
        const EXDEV: i32 = 18;
        // Raw EXDEV (no matching kind) is detected by the std-side helper.
        let exdev = std::io::Error::from_raw_os_error(EXDEV);
        assert!(is_std_cross_device(&exdev), "raw EXDEV must be recognised");
    }

    // ErrorKind::CrossesDevices is the modern stable variant (Rust 1.85+).
    let crosses = io::Error::from(io::ErrorKind::CrossesDevices);
    assert!(is_cross_device(&crosses));

    // ErrorKind::Unsupported covers Windows / exotic filesystems where
    // hard links are simply not implemented - also a cue to fall back.
    let unsupported = io::Error::from(io::ErrorKind::Unsupported);
    assert!(is_cross_device(&unsupported));

    // A garden-variety NotFound must NOT be misclassified - the caller
    // needs to surface that error verbatim, not silently copy.
    let notfound = io::Error::from(io::ErrorKind::NotFound);
    assert!(!is_cross_device(&notfound));
}

#[test]
fn fadvise_accepts_every_hint_variant() -> io::Result<()> {
    // Smoke test: every FileHint variant produces Ok on every
    // platform our CI builds for. Linux exercises the real
    // posix_fadvise syscall; macOS / Windows fall through the
    // no-op branches. Failure of any variant is a regression in
    // the platform-specific glue, not in the hint contract
    // itself (which is advisory).
    let dir = tempfile::tempdir()?;
    let fs = StdFs;
    let path = dir.path().join("hint_smoke.bin");

    // Write some content so posix_fadvise has a non-empty file
    // to act on (POSIX_FADV_DONTNEED is a no-op on 0-byte files
    // on every kernel I've tested, but checking with content
    // catches more potential regressions).
    let opts = FsOpenOptions::new().write(true).create_new(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(&vec![0u8; 64 * 1024])?;
    file.sync_all()?;
    drop(file);

    let opts = FsOpenOptions::new().read(true);
    let file = fs.open(&path, &opts)?;
    for hint in [
        FileHint::Default,
        FileHint::Sequential,
        FileHint::Random,
        FileHint::WriteOnce,
    ] {
        file.hint(hint)?;
    }
    Ok(())
}

/// On macOS the capability profile for any real directory must be either
/// the full APFS profile (copy-on-write + reflink + native snapshot - the
/// common case, including CI runners and `/var/folders` temp dirs) or the
/// conservative all-false default (non-APFS mount). Never a partial mix.
#[cfg(target_os = "macos")]
#[test]
fn capabilities_macos_is_apfs_profile_or_conservative() {
    use crate::fs::FsCapabilities;
    let dir = tempfile::tempdir().unwrap();
    let caps = StdFs.capabilities(dir.path());
    let apfs = FsCapabilities {
        copy_on_write: true,
        reflink: true,
        native_snapshot: true,
        ..FsCapabilities::default()
    };
    assert!(
        caps == apfs || caps == FsCapabilities::default(),
        "unexpected macOS capability profile: {caps:?}"
    );
}

/// macOS `reflink_file` must produce an independent, byte-identical clone
/// whether it went through `clonefile(2)` (APFS) or the byte-copy fallback:
/// the clone matches the source, and a later overwrite of the source does
/// not change the clone.
#[cfg(target_os = "macos")]
#[test]
fn reflink_file_macos_produces_independent_identical_copy() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("src.bin");
    let dst = dir.path().join("clone.bin");
    let fs = StdFs;

    let mut f = fs.open(&src, &FsOpenOptions::new().write(true).create_new(true))?;
    f.write_all(b"reflink-source-data")?;
    f.sync_all()?;
    drop(f);

    fs.reflink_file(&src, &dst)?;

    let mut buf = Vec::new();
    fs.open(&dst, &FsOpenOptions::new().read(true))?
        .read_to_end(&mut buf)?;
    assert_eq!(buf, b"reflink-source-data");

    // Overwrite the source; the clone must be unaffected (clonefile shares
    // blocks copy-on-write, the fallback is a separate file - either way
    // independent).
    let mut w = fs.open(&src, &FsOpenOptions::new().write(true).truncate(true))?;
    w.write_all(b"changed")?;
    w.sync_all()?;
    drop(w);

    let mut after = Vec::new();
    fs.open(&dst, &FsOpenOptions::new().read(true))?
        .read_to_end(&mut after)?;
    assert_eq!(
        after, b"reflink-source-data",
        "reflink clone must be independent"
    );
    Ok(())
}

/// Linux `reflink_file` must produce an independent, byte-identical clone
/// on any filesystem: `ioctl(FICLONE)` on Btrfs / XFS-reflink, the byte-copy
/// fallback on ext4 / tmpfs. Robust regardless of the CI runner's FS.
#[cfg(target_os = "linux")]
#[test]
fn reflink_file_linux_produces_independent_identical_copy() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("src.bin");
    let dst = dir.path().join("clone.bin");
    let fs = StdFs;

    let mut f = fs.open(&src, &FsOpenOptions::new().write(true).create_new(true))?;
    f.write_all(b"reflink-source-data")?;
    f.sync_all()?;
    drop(f);

    fs.reflink_file(&src, &dst)?;

    let mut buf = Vec::new();
    fs.open(&dst, &FsOpenOptions::new().read(true))?
        .read_to_end(&mut buf)?;
    assert_eq!(buf, b"reflink-source-data");

    let mut w = fs.open(&src, &FsOpenOptions::new().write(true).truncate(true))?;
    w.write_all(b"changed")?;
    w.sync_all()?;
    drop(w);

    let mut after = Vec::new();
    fs.open(&dst, &FsOpenOptions::new().read(true))?
        .read_to_end(&mut after)?;
    assert_eq!(
        after, b"reflink-source-data",
        "reflink clone must be independent"
    );
    Ok(())
}

/// Linux `try_disable_cow` must succeed on any filesystem: it sets
/// `FS_NOCOW_FL` on Btrfs and is a graceful no-op elsewhere (the
/// inode-flags ioctl is unsupported on tmpfs / ext4-without-the-flag), never
/// surfacing an error for a missing optimization.
#[cfg(target_os = "linux")]
#[test]
fn try_disable_cow_linux_succeeds_or_noops() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("sst.bin");
    let fs = StdFs;
    // Empty file, as at SST creation time (the flag only takes on an empty
    // file).
    fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
    fs.try_disable_cow(&path)?;
    Ok(())
}
