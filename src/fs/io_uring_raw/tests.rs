use super::*;

// These tests require a Linux kernel with io_uring; they are compiled only
// under `cfg(target_os = "linux")` (the module gate) and run on the Linux
// CI / bench runner, not on the macOS dev host.

#[test]
fn raw_file_fsfile_round_trips() {
    // Exercises the full IoUringRawFile FsFile surface: sequential write
    // (ring), positioned read_at (ring, fill-or-EOF), metadata (statx),
    // Seek + sequential Read (cursor), set_len (ftruncate), fsync (ring),
    // and try_lock_exclusive (flock) — all over the raw no_std driver.
    use std::io::{Read, Seek, Write};

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("iou_rawfile.bin");
    let cpath = std::ffi::CString::new(path.to_str().expect("utf8 path"))
        .expect("path has no interior NUL");

    let fd = open_raw(&cpath, O_CREAT | O_RDWR | O_TRUNC, 0o600).expect("openat should succeed");
    let ring = Arc::new(Mutex::new(IoUringRaw::new(8).expect("ring setup")));
    let mut file = IoUringRawFile::new(ring, fd, false);

    let payload = b"raw io_uring file round-trip payload";
    let written = Write::write(&mut file, payload).expect("write");
    assert_eq!(written, payload.len(), "short write");

    // statx-backed metadata reflects the written length.
    assert_eq!(
        FsFile::metadata(&file).expect("metadata").len,
        payload.len() as u64,
        "metadata len must match what was written"
    );

    // Positioned fill-or-EOF read returns the exact bytes.
    let mut rb = vec![0u8; payload.len()];
    let n = FsFile::read_at(&file, &mut rb, 0).expect("read_at");
    assert_eq!(n, payload.len(), "short read_at");
    assert_eq!(&rb, payload, "read_at bytes must match");

    // Seek + sequential Read from an offset uses the userspace cursor.
    file.seek(std::io::SeekFrom::Start(4)).expect("seek");
    let mut tail = vec![0u8; payload.len() - 4];
    let mut got = 0;
    while got < tail.len() {
        let chunk = tail.get_mut(got..).expect("in-bounds");
        let r = Read::read(&mut file, chunk).expect("read");
        if r == 0 {
            break;
        }
        got += r;
    }
    assert_eq!(
        tail.get(..got).expect("in-bounds"),
        payload.get(4..).expect("in-bounds"),
        "seek+read bytes must match"
    );

    FsFile::sync_all(&file).expect("sync_all");
    FsFile::sync_data(&file).expect("sync_data");

    // ftruncate shrinks the file; metadata reflects it.
    FsFile::set_len(&file, 4).expect("set_len");
    assert_eq!(
        FsFile::metadata(&file).expect("metadata after set_len").len,
        4,
        "set_len must shrink the file"
    );

    // A fresh file is unlocked, so a non-blocking exclusive lock succeeds.
    assert!(
        FsFile::try_lock_exclusive(&file).expect("try_lock_exclusive"),
        "exclusive lock on a fresh file must succeed"
    );

    drop(file); // closes the fd; `tmp` drops at end of scope, removing the dir
}

#[test]
fn raw_fs_directory_and_file_ops() {
    // Exercises the IoUringRawFs Fs surface end to end: create_dir_all
    // (mkdirat), open + write (ring), metadata + exists (statx), read_dir
    // (getdents64), rename (renameat2), remove_file + remove_dir_all
    // (unlinkat) — all over the raw no_std driver.
    use crate::fs::Fs;
    use crate::path::Path;
    use std::io::Write;

    // A subdirectory under a `tempfile::tempdir()` guard: `create_dir_all`
    // creates it, and the guard removes the whole tree on drop even if an
    // assertion panics before the explicit `remove_dir_all` below.
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = tmp.path().join("rawfs");
    let base_s = base.to_str().expect("utf8 temp path");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let dir = Path::new(base_s);

    fs.create_dir_all(dir).expect("create_dir_all");
    assert!(fs.exists(dir).expect("dir exists"));
    assert!(fs.metadata(dir).expect("dir metadata").is_dir);

    // Open + write a file through the ring.
    let fpath = dir.join("a.txt");
    let mut file = fs
        .open(&fpath, &FsOpenOptions::new().write(true).create(true))
        .expect("open");
    file.write_all(b"hello").expect("write");
    file.sync_all().expect("sync");
    drop(file);

    // statx-backed metadata + getdents64 listing see the file.
    assert_eq!(fs.metadata(&fpath).expect("file metadata").len, 5);
    let entries = fs.read_dir(dir).expect("read_dir");
    assert!(
        entries.iter().any(|e| e.file_name == "a.txt" && !e.is_dir),
        "read_dir must list the written file"
    );

    // rename (renameat2) moves it.
    let fpath2 = dir.join("b.txt");
    fs.rename(&fpath, &fpath2).expect("rename");
    assert!(!fs.exists(&fpath).expect("old gone"));
    assert!(fs.exists(&fpath2).expect("new present"));

    // unlinkat removes the file, then the (now empty) directory.
    fs.remove_file(&fpath2).expect("remove_file");
    assert!(!fs.exists(&fpath2).expect("file gone"));
    fs.remove_dir_all(dir).expect("remove_dir_all");
    assert!(!fs.exists(dir).expect("dir gone"));
}

#[test]
fn raw_file_append_writes_accumulate_at_eof() {
    // O_APPEND writes always land at end of file: two sequential appends
    // (across reopens) both persist, in order — exercising the kernel's
    // atomic-append path (offset -1) the write impl uses for append mode.
    use crate::fs::Fs;
    use crate::path::Path;
    use std::io::Write;

    let tmp = tempfile::tempdir().expect("tempdir");
    let p = tmp.path().join("log.bin");
    let ps = p.to_str().expect("utf8 temp path");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let path = Path::new(ps);

    {
        let mut f = fs
            .open(path, &FsOpenOptions::new().append(true).create(true))
            .expect("open append");
        f.write_all(b"aaa").expect("first append");
        f.sync_all().expect("sync");
    }
    {
        let mut f = fs
            .open(path, &FsOpenOptions::new().append(true).create(true))
            .expect("reopen append");
        f.write_all(b"bbb").expect("second append");
        f.sync_all().expect("sync");
    }

    let f = fs
        .open(path, &FsOpenOptions::new().read(true))
        .expect("open read");
    let mut buf = [0u8; 6];
    let n = f.read_at(&mut buf, 0).expect("read_at");
    assert_eq!(n, 6, "both appends must be present");
    assert_eq!(&buf, b"aaabbb", "appends land in order at EOF");
}

#[test]
fn ring_setup_and_nop_round_trips() {
    let mut ring = IoUringRaw::new(8).expect("io_uring_setup + mmap should succeed");
    assert!(
        ring.sq_entries() >= 8,
        "kernel rounds entries up to >= request"
    );
    // A NOP completes immediately with res == 0 and echoes our user_data
    // slot through the full submit -> enter -> complete cycle.
    let res = ring
        .nop(0xDEAD_BEEF)
        .expect("NOP submit/complete should succeed");
    assert_eq!(res, 0, "NOP res must be 0");
}

#[test]
fn multiple_nops_reuse_slots() {
    let mut ring = IoUringRaw::new(4).expect("setup");
    // Submit more NOPs than the ring depth to exercise tail/head wraparound
    // and slot reuse across several enter calls.
    for i in 0..16u64 {
        let res = ring.nop(i).expect("nop");
        assert_eq!(res, 0);
    }
}

#[test]
fn file_write_fsync_read_round_trips_through_the_ring() {
    // Real data path: open a file (raw openat), write a payload at an
    // offset through the ring, fsync it through the ring, read it back
    // through the ring, and verify the bytes — then close (raw) + clean up.
    let path = std::env::temp_dir().join(format!(
        "iou_raw_rt_{}_{}.bin",
        std::process::id(),
        // a per-test suffix so parallel runs do not collide
        line!()
    ));
    let cpath = std::ffi::CString::new(path.to_str().expect("utf8 path"))
        .expect("path has no interior NUL");

    let fd = open_raw(&cpath, O_CREAT | O_RDWR | O_TRUNC, 0o600).expect("openat should succeed");

    let mut ring = IoUringRaw::new(8).expect("ring setup");

    let payload = b"io_uring raw round-trip payload";
    let offset = 4096u64; // a non-zero offset to exercise positioned I/O

    let written = ring
        .write_at(fd, payload, offset)
        .expect("write_at should succeed");
    assert_eq!(written, payload.len(), "short write");

    ring.fsync(fd, false).expect("fsync should succeed");

    let mut readback = vec![0u8; payload.len()];
    let read = ring
        .read_at(fd, &mut readback, offset)
        .expect("read_at should succeed");
    assert_eq!(read, payload.len(), "short read");
    assert_eq!(
        &readback, payload,
        "read-back bytes must match what we wrote"
    );

    // Reading past EOF returns 0 bytes (the file is exactly offset+len long).
    let mut tail = [0u8; 8];
    let eof = ring
        .read_at(fd, &mut tail, offset + payload.len() as u64)
        .expect("read at EOF should succeed");
    assert_eq!(eof, 0, "read past EOF must return 0");

    close_raw(fd).expect("close should succeed");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn errno_maps_to_expected_error_kinds() {
    // Every explicitly-mapped errno lands on its kind; anything else folds
    // into `Other`. Covers the whole `errno_to_kind` table without needing
    // to provoke each real failure.
    assert_eq!(errno_to_kind(1), ErrorKind::PermissionDenied); // EPERM
    assert_eq!(errno_to_kind(2), ErrorKind::NotFound); // ENOENT
    assert_eq!(errno_to_kind(4), ErrorKind::Interrupted); // EINTR
    assert_eq!(errno_to_kind(9), ErrorKind::InvalidInput); // EBADF
    assert_eq!(errno_to_kind(11), ErrorKind::WouldBlock); // EAGAIN
    assert_eq!(errno_to_kind(13), ErrorKind::PermissionDenied); // EACCES
    assert_eq!(errno_to_kind(17), ErrorKind::AlreadyExists); // EEXIST
    assert_eq!(errno_to_kind(22), ErrorKind::InvalidInput); // EINVAL
    assert_eq!(errno_to_kind(95), ErrorKind::Unsupported); // EOPNOTSUPP
    assert_eq!(errno_to_kind(132), ErrorKind::Other); // unmapped
}

#[test]
fn ring_setup_with_zero_entries_is_rejected() {
    // `io_uring_setup` rejects a zero-entry ring with EINVAL; this covers
    // the setup error path (and the fd guard never arming). `IoUringRaw`
    // is not `Debug`, so match rather than `expect_err`.
    match IoUringRaw::new(0) {
        Ok(_) => panic!("zero-entry ring must be rejected"),
        Err(e) => assert_eq!(e.kind(), ErrorKind::InvalidInput),
    }
}

#[test]
fn open_raw_missing_file_returns_not_found() {
    // Opening a missing path read-only (no O_CREAT) fails with ENOENT,
    // exercising `open_raw`'s error path and the `err` -> `ErrorKind`
    // conversion.
    let cpath =
        std::ffi::CString::new("/proc/does-not-exist/iou_raw_missing").expect("no interior NUL");
    let err = open_raw(&cpath, O_RDONLY, 0).expect_err("missing file must fail to open");
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[test]
fn ring_op_on_bad_fd_surfaces_completion_error() {
    // A write against a descriptor that is not open completes with -EBADF;
    // the reaper must surface that negative `res` as an error (covering the
    // `res < 0` branch of `submit_and_reap_one`).
    let mut ring = IoUringRaw::new(4).expect("ring setup");
    let err = ring
        .write_at(1 << 30, b"x", 0)
        .expect_err("write to a non-open fd must fail");
    assert_eq!(err.kind(), ErrorKind::InvalidInput); // EBADF -> InvalidInput
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[test]
fn raw_available_space_reports_plausible_free_bytes() {
    // The raw `statfs` syscall path: the filesystem backing the tempdir must
    // report a plausible, non-zero free figure below the unbounded sentinel.
    use crate::fs::Fs;
    use crate::path::Path;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let free = fs
        .available_space(Path::new(tmp.path().to_str().expect("utf8 path")))
        .expect("statfs must succeed on a real filesystem");
    assert!(
        free > 0,
        "a writable tempdir filesystem must report free space"
    );
    assert!(
        free < u64::MAX,
        "a real probe must not return the unbounded sentinel"
    );
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[test]
fn raw_statfs_on_missing_path_errors() {
    // `statfs` on a path that does not exist surfaces an error, not a silent
    // zero or the unbounded sentinel — exercising the syscall error branch.
    let cpath =
        std::ffi::CString::new("/proc/does-not-exist/iou_raw_statfs").expect("no interior NUL");
    assert!(statfs_available_raw(&cpath).is_err());
}
