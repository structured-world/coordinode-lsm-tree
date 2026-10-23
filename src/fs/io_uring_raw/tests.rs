use super::*;

// These tests require a Linux kernel with io_uring; they are compiled only
// under `cfg(target_os = "linux")` (the module gate) and run on the Linux
// CI / bench runner, not on the macOS dev host.

// A ring shallower than most batches below, so they go through it in waves.
#[cfg(feature = "std")]
crate::fs::batch_conformance::batched_read_conformance!(Some(
    super::IoUringRawFs::new(8).expect("ring setup")
));

/// A batch deeper than the ring goes through it in waves, every read landing
/// in its own buffer, in far fewer entries into the kernel than reads.
#[cfg(feature = "std")]
#[test]
fn a_batch_deeper_than_the_ring_reads_every_block_in_few_enters() {
    use crate::fs::{BlockBuf, BlockRead};
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(4).expect("ring setup");
    let path = Path::new(tmp.path().to_str().expect("utf8 path")).join("deep.bin");
    let bytes: Vec<u8> = (0..64 * 16)
        .map(|i| u8::try_from(i % 251).expect("below 251"))
        .collect();
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts).expect("open");
    file.write_all(&bytes).expect("write");

    let mut buffers = vec![[0u8; 16]; 64];
    let enters = fs.ring.lock().enters;
    {
        // Reversed, so a read landing in another's buffer is caught.
        let mut reqs: Vec<_> = buffers
            .iter_mut()
            .enumerate()
            .rev()
            .map(|(i, buf)| BlockRead {
                file: file.as_ref(),
                offset: (i * 16) as u64,
                buf: BlockBuf::new(buf),
            })
            .collect();
        fs.read_blocks_batched(&mut reqs).expect("batched read");
    }
    let taken = fs.ring.lock().enters - enters;
    for (i, buf) in buffers.iter().enumerate() {
        assert_eq!(&buf[..], bytes.get(i * 16..(i + 1) * 16).expect("in range"));
    }
    assert!(
        taken <= 64 / 4 * 2,
        "64 reads through a 4-entry ring took {taken} enters"
    );
}

/// A batched read of a descriptor that is not open completes with `-EBADF`,
/// which the batch reports as the read's error; a read returning fewer bytes
/// than its block is a block cut off, not an end of file.
#[test]
fn a_batched_read_reports_an_errno_and_a_short_block() {
    let mut ring = IoUringRaw::new(4).expect("ring setup");
    let mut buf = [0u8; 8];
    let reads = [RawRead {
        fd: 1 << 30,
        buf: buf.as_mut_ptr(),
        len: 8,
        offset: 0,
    }];
    let mut results = [0i32];
    // SAFETY: `buf` outlives the call, which returns once the read completed.
    unsafe { ring.read_batch(&reads, &mut results) };
    let [res] = results;
    let error = read_verdict(res, 8).expect_err("a read of no descriptor fails");
    assert_eq!(error.kind(), ErrorKind::InvalidInput, "EBADF: {error}");

    let short = read_verdict(4, 8).expect_err("half a block is no block");
    assert_eq!(short.kind(), ErrorKind::UnexpectedEof);
    assert!(read_verdict(8, 8).is_ok());
}

/// `read_many` on one file is one batch too.
#[cfg(feature = "std")]
#[test]
fn read_many_is_one_batch_of_the_ring() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(32).expect("ring setup");
    let path = Path::new(tmp.path().to_str().expect("utf8 path")).join("many.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts).expect("open");
    file.write_all(&[7u8; 16 * 16]).expect("write");

    let mut buffers = vec![[0u8; 16]; 16];
    let enters = fs.ring.lock().enters;
    let mut regions: Vec<(u64, &mut [u8])> = buffers
        .iter_mut()
        .enumerate()
        .map(|(i, buf)| ((i * 16) as u64, &mut buf[..]))
        .collect();
    file.read_many(&mut regions).expect("read_many");
    drop(regions);
    assert!(
        fs.ring.lock().enters - enters <= 2,
        "16 regions within the ring's depth go in one or two enters"
    );
    assert!(buffers.iter().all(|buf| buf == &[7u8; 16]));
}

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

    // statx-backed hard-link count: a freshly created file has exactly one link
    // (not the `Unsupported` default that would make in-place heal skip reclaim).
    assert_eq!(
        FsFile::hard_link_count(&file).expect("hard_link_count"),
        1,
        "a freshly created file has a single hard link"
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

/// Two hard links of one inode are one file and a separate file with the same
/// bytes is not, by device and inode as the other kernel backends answer; a
/// name that does not exist has no identity, and the probe says so.
#[test]
fn raw_fs_same_file_answers_by_identity() {
    use crate::fs::Fs;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    let c = tmp.path().join("c");
    std::fs::write(&a, b"payload").expect("write a");
    std::fs::hard_link(&a, &b).expect("link b");
    std::fs::write(&c, b"payload").expect("write c");

    assert!(
        fs.same_file(&a, &b).expect("probe"),
        "two names of one inode"
    );
    assert!(!fs.same_file(&a, &c).expect("probe"), "two inodes");
    let missing = fs
        .same_file(&a, &tmp.path().join("missing"))
        .expect_err("a missing name has no identity");
    assert_eq!(missing.kind(), ErrorKind::NotFound);
}

/// A symlink's target is read as stored, without following it; a regular
/// file and a missing name are not symlinks.
#[test]
fn raw_fs_read_link_reports_only_symlinks() {
    use crate::fs::Fs;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let file = tmp.path().join("file");
    let link = tmp.path().join("link");
    std::fs::write(&file, b"payload").expect("write");
    std::os::unix::fs::symlink("dangling", &link).expect("symlink");

    assert_eq!(
        fs.read_link(&link).expect("read_link"),
        Some(crate::path::PathBuf::from("dangling"))
    );
    assert_eq!(fs.read_link(&file).expect("read_link"), None);
    // A target that is not UTF-8 comes back as its bytes.
    let raw = tmp.path().join("raw");
    {
        use std::os::unix::ffi::OsStrExt as _;
        std::os::unix::fs::symlink(std::ffi::OsStr::from_bytes(b"t\xff"), &raw).expect("symlink");
        assert_eq!(
            fs.read_link(&raw).expect("read_link"),
            Some(std::path::PathBuf::from(std::ffi::OsStr::from_bytes(
                b"t\xff"
            )))
        );
    }
    assert_eq!(
        fs.read_link(&tmp.path().join("missing"))
            .expect("read_link"),
        None
    );
}

/// A symbolic link the raw backend makes holds its target as given, so a
/// crash simulator over this backend can put a lost link back.
#[test]
fn raw_fs_symlink_makes_a_link_read_link_reads_back() {
    use crate::fs::Fs;

    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let link = tmp.path().join("link");
    fs.symlink(std::path::Path::new("elsewhere"), &link, false)
        .expect("symlink");
    assert_eq!(
        fs.read_link(&link).expect("read_link"),
        Some(crate::path::PathBuf::from("elsewhere"))
    );
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
fn raw_fs_no_wait_refuses_a_pipe_with_no_reader_and_opens_a_file() {
    // `no_wait` fails the open of a named pipe nothing reads rather than
    // block it, and still opens a regular file, which then writes as any.
    use crate::fs::Fs;
    use crate::path::Path;
    use std::io::Write;

    let tmp = tempfile::tempdir().expect("tempdir");
    let pipe = tmp.path().join("pipe");
    let file = tmp.path().join("file");
    let name = std::ffi::CString::new(pipe.to_str().expect("utf8")).expect("no NUL");
    // SAFETY: `name` is a NUL-terminated path the call only reads.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");
    std::fs::write(&file, b"").expect("file");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let opts = FsOpenOptions::new().write(true).no_wait(true);

    assert!(
        fs.open(Path::new(pipe.to_str().expect("utf8")), &opts)
            .is_err(),
        "a pipe with no reader is refused"
    );
    let mut opened = fs
        .open(Path::new(file.to_str().expect("utf8")), &opts)
        .expect("a regular file opens");
    opened.write_all(b"a table").expect("write");
    drop(opened);
    assert_eq!(std::fs::read(&file).expect("read"), b"a table");
}

#[test]
fn raw_fs_no_follow_refuses_a_symlink_and_opens_a_file() {
    // `no_follow` refuses a symbolic link at the last component (`ELOOP`),
    // so nothing is written through it into the file it names, and still
    // opens a regular file.
    use crate::fs::Fs;
    use crate::path::Path;

    let tmp = tempfile::tempdir().expect("tempdir");
    let target = tmp.path().join("target");
    let link = tmp.path().join("link");
    std::fs::write(&target, b"").expect("target");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let opts = FsOpenOptions::new().write(true).no_follow(true);

    assert!(
        fs.open(Path::new(link.to_str().expect("utf8")), &opts)
            .is_err(),
        "a symlink is refused"
    );
    assert!(
        fs.open(Path::new(target.to_str().expect("utf8")), &opts)
            .is_ok(),
        "a regular file opens"
    );
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
    // Arch-specific (95 asm-generic, 122 MIPS, 45 SPARC).
    assert_eq!(errno_to_kind(libc::EOPNOTSUPP), ErrorKind::Unsupported);
    assert_eq!(errno_to_kind(132), ErrorKind::Other); // unmapped
}

/// A failure of the environment rather than of the file (no descriptors, no
/// memory, no space, a dropped network mount) must reach recovery grading as
/// an environmental kind: read as `Other`, it condemns an intact table.
#[test]
fn environment_errnos_map_to_environmental_kinds() {
    for (errno, kind) in [
        (libc::ENOMEM, ErrorKind::OutOfMemory),
        (libc::EMFILE, ErrorKind::TooManyOpenFiles),
        (libc::ENFILE, ErrorKind::TooManyOpenFiles),
        (libc::ENOSPC, ErrorKind::StorageFull),
        (libc::EROFS, ErrorKind::ReadOnlyFilesystem),
        (libc::EDQUOT, ErrorKind::QuotaExceeded),
        (libc::EPIPE, ErrorKind::BrokenPipe),
        (libc::ENETDOWN, ErrorKind::NetworkDown),
        (libc::ENETUNREACH, ErrorKind::NetworkUnreachable),
        (libc::ECONNABORTED, ErrorKind::ConnectionAborted),
        (libc::ECONNRESET, ErrorKind::ConnectionReset),
        (libc::ENOTCONN, ErrorKind::NotConnected),
        (libc::ETIMEDOUT, ErrorKind::TimedOut),
        (libc::ECONNREFUSED, ErrorKind::ConnectionRefused),
        (libc::EHOSTUNREACH, ErrorKind::HostUnreachable),
        (libc::ESTALE, ErrorKind::StaleNetworkFileHandle),
    ] {
        assert_eq!(errno_to_kind(errno), kind, "errno {errno}");
        assert!(kind.is_environmental(), "{kind:?} for errno {errno}");
    }
    assert_eq!(errno_to_kind(libc::EXDEV), ErrorKind::CrossesDevices);
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

// The capability-dependent surface answers as `StdFs` does on the same files:
// a tree on the raw backend keeps the reclaim, clone and volume decisions the
// std backend makes.

/// Writes `len` bytes of `byte` to a new file at `path` through `fs`.
#[cfg(feature = "std")]
fn write_raw_file(fs: &IoUringRawFs, path: &std::path::Path, byte: u8, len: usize) {
    use std::io::Write as _;
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).create(true))
        .expect("open");
    file.write_all(&alloc::vec![byte; len]).expect("write");
    file.sync_all().expect("sync");
}

/// A directory reports the same capabilities through the raw backend as
/// through `StdFs`: every path that adapts to the file system (tight-space
/// reclaim, prefix punches, reflinked checkpoints) asks them first.
#[cfg(all(feature = "std", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[test]
fn raw_capabilities_match_the_std_backend() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    assert_eq!(
        fs.capabilities(tmp.path()),
        crate::fs::StdFs.capabilities(tmp.path())
    );
}

/// A hole punched through the raw backend frees the range as `StdFs` sees it:
/// the range reads as zeros, the length is kept, both hole probes and the
/// allocated size answer as they do through `StdFs`.
#[cfg(all(feature = "std", target_pointer_width = "64"))]
#[test]
fn a_hole_punched_through_the_raw_backend_lands() {
    let tmp = tempfile::tempdir().expect("tempdir");
    if !crate::fs::StdFs.capabilities(tmp.path()).punch_hole {
        eprintln!("skipping: the temporary directory's file system cannot punch holes");
        return;
    }
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let std_fs = crate::fs::StdFs;
    let path = tmp.path().join("punched");
    write_raw_file(&fs, &path, 0xAB, 1 << 20);
    fs.punch_hole(&path, 0, 512 * 1024).expect("punch_hole");

    assert_eq!(
        fs.extent_contains_hole(&path, 0, 512 * 1024)
            .expect("probe"),
        Some(true)
    );
    for (offset, len) in [(0, 512 * 1024), (512 * 1024, 4_096), (1 << 20, 4_096)] {
        assert_eq!(
            fs.extent_is_hole(&path, offset, len).expect("probe"),
            std_fs.extent_is_hole(&path, offset, len).expect("probe"),
            "extent_is_hole({offset}, {len})"
        );
        assert_eq!(
            fs.extent_contains_hole(&path, offset, len).expect("probe"),
            std_fs
                .extent_contains_hole(&path, offset, len)
                .expect("probe"),
            "extent_contains_hole({offset}, {len})"
        );
    }
    assert_eq!(
        fs.allocated_size(&path).expect("allocated_size"),
        std_fs.allocated_size(&path).expect("allocated_size")
    );
    assert_eq!(fs.metadata(&path).expect("metadata").len, 1 << 20);
    let file = fs
        .open(&path, &FsOpenOptions::new().read(true))
        .expect("open");
    let mut head = [0xFFu8; 64];
    assert_eq!(file.read_at(&mut head, 0).expect("read"), 64);
    assert_eq!(head, [0u8; 64], "the punched range reads as zeros");
    let mut tail = [0u8; 64];
    assert_eq!(file.read_at(&mut tail, 512 * 1024).expect("read"), 64);
    assert_eq!(tail, [0xABu8; 64], "the rest is untouched");
}

/// A fully written file holds no hole, and a range past its end is a hole that
/// none of the file's bytes can sit in, as `StdFs` answers; a file that does
/// not exist is an error for the allocated size, never "cannot tell".
#[cfg(all(feature = "std", target_pointer_width = "64"))]
#[test]
fn the_raw_extent_probes_answer_as_the_std_backend() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let std_fs = crate::fs::StdFs;
    let path = tmp.path().join("dense");
    write_raw_file(&fs, &path, 1, 64 * 1024);

    for (offset, len) in [(0, 64 * 1024), (4_096, 1), (64 * 1024, 4_096), (0, 0)] {
        assert_eq!(
            fs.extent_is_hole(&path, offset, len).expect("probe"),
            std_fs.extent_is_hole(&path, offset, len).expect("probe"),
            "extent_is_hole({offset}, {len})"
        );
        assert_eq!(
            fs.extent_contains_hole(&path, offset, len).expect("probe"),
            std_fs
                .extent_contains_hole(&path, offset, len)
                .expect("probe"),
            "extent_contains_hole({offset}, {len})"
        );
    }
    let allocated = fs.allocated_size(&path).expect("allocated_size");
    assert!(allocated.is_some(), "a written file reports its allocation");
    assert_eq!(
        allocated,
        std_fs.allocated_size(&path).expect("allocated_size")
    );
    let missing = tmp.path().join("missing");
    assert_eq!(
        fs.allocated_size(&missing)
            .expect_err("no such file")
            .kind(),
        ErrorKind::NotFound
    );
}

/// A hard link through the raw backend is a second name of the inode, its
/// link count reads by path, and the refusals are the ones `StdFs` gives: an
/// existing destination, a missing source.
#[cfg(feature = "std")]
#[test]
fn raw_hard_links_count_and_refuse_as_the_std_backend() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let (path, link) = (tmp.path().join("table"), tmp.path().join("link"));
    write_raw_file(&fs, &path, 1, 4_096);
    assert_eq!(fs.hard_link_count(&path).expect("count"), 1);
    fs.hard_link(&path, &link).expect("hard_link");
    assert_eq!(fs.hard_link_count(&path).expect("count"), 2);
    assert!(fs.same_file(&path, &link).expect("same_file"));

    assert_eq!(
        fs.hard_link(&path, &link).expect_err("exists").kind(),
        ErrorKind::AlreadyExists
    );
    let missing = tmp.path().join("missing");
    assert_eq!(
        fs.hard_link(&missing, &tmp.path().join("other"))
            .expect_err("no source")
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        fs.hard_link_count(&missing).expect_err("no file").kind(),
        ErrorKind::NotFound
    );
}

/// The raw backend resolves paths through the host kernel as `StdFs` does, so
/// it names the same namespace and the same volumes: a checkpoint hard-links
/// across the two and the space gate tells mounts apart.
#[cfg(feature = "std")]
#[test]
fn raw_namespace_and_volume_ids_match_the_std_backend() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    assert_eq!(fs.backend_id(), crate::fs::StdFs.backend_id());
    assert!(fs.backend_id().is_some());
    let volume = fs.volume_id(tmp.path());
    assert!(volume.is_some(), "a real mount has an id");
    assert_eq!(volume, crate::fs::StdFs.volume_id(tmp.path()));
    assert_eq!(fs.volume_id(&tmp.path().join("missing")), None);
}

/// A file cloned through the raw backend holds the source's bytes in an inode
/// of its own, whether the file system shares the blocks or the clone falls
/// back to a copy; an existing destination is refused.
#[cfg(feature = "std")]
#[test]
fn a_file_cloned_through_the_raw_backend_is_independent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let (source, clone) = (tmp.path().join("source"), tmp.path().join("clone"));
    write_raw_file(&fs, &source, 7, 64 * 1024);
    fs.reflink_file(&source, &clone).expect("reflink_file");

    let clone_file = fs
        .open(&clone, &FsOpenOptions::new().read(true).write(true))
        .expect("open");
    let mut bytes = alloc::vec![0u8; 64 * 1024];
    assert_eq!(
        clone_file.read_at(&mut bytes, 0).expect("read"),
        bytes.len()
    );
    assert!(bytes.iter().all(|&b| b == 7), "the clone holds the source");
    assert_eq!(
        fs.hard_link_count(&clone).expect("count"),
        1,
        "an inode of its own"
    );
    assert_eq!(
        fs.reflink_file(&source, &clone).expect_err("exists").kind(),
        ErrorKind::AlreadyExists
    );
}

/// Turning copy-on-write off for a new file succeeds where the file system
/// has the flag and is a no-op where it has not, as through `StdFs`.
#[cfg(all(
    feature = "std",
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    )
))]
#[test]
fn raw_disabling_copy_on_write_answers_as_the_std_backend() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let (raw_path, std_path) = (tmp.path().join("raw"), tmp.path().join("std"));
    write_raw_file(&fs, &raw_path, 0, 0);
    write_raw_file(&fs, &std_path, 0, 0);
    assert_eq!(
        fs.try_disable_cow(&raw_path).map_err(|e| e.kind()),
        crate::fs::StdFs
            .try_disable_cow(&std_path)
            .map_err(|e| e.kind())
    );
    assert_eq!(
        fs.try_disable_cow(&tmp.path().join("missing"))
            .expect_err("no file")
            .kind(),
        ErrorKind::NotFound
    );
}

/// Every access hint reaches the kernel through the raw backend's file and is
/// accepted, as `StdFs` accepts it.
#[cfg(all(feature = "std", target_pointer_width = "64"))]
#[test]
fn raw_files_accept_every_access_hint() {
    use crate::fs::FileHint;
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let path = tmp.path().join("hinted");
    write_raw_file(&fs, &path, 3, 4_096);
    let file = fs
        .open(&path, &FsOpenOptions::new().read(true))
        .expect("open");
    for hint in [
        FileHint::Default,
        FileHint::Sequential,
        FileHint::Random,
        FileHint::WriteOnce,
    ] {
        file.hint(hint).expect("hint");
    }

    // A pipe takes no access advice: the kernel refuses it, and the refusal
    // reaches the caller as through `StdFs`.
    let pipe = tmp.path().join("pipe");
    let name = std::ffi::CString::new(pipe.to_str().expect("utf8")).expect("no NUL");
    // SAFETY: `name` is a NUL-terminated path the call only reads.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");
    let opts = FsOpenOptions::new().read(true).no_wait(true);
    let raw_pipe = fs.open(&pipe, &opts).expect("open the pipe");
    let std_pipe = crate::fs::StdFs.open(&pipe, &opts).expect("open the pipe");
    assert!(std_pipe.hint(FileHint::Sequential).is_err());
    assert!(raw_pipe.hint(FileHint::Sequential).is_err());
}

/// A range no file extent can have (past `i64::MAX`, or ending past
/// `u64::MAX`) is refused before any syscall, and a file that does not exist
/// is not found, for the punch and both hole probes, as through `StdFs`.
#[cfg(all(feature = "std", target_pointer_width = "64"))]
#[test]
fn raw_range_syscalls_refuse_what_the_std_backend_refuses() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let std_fs = crate::fs::StdFs;
    let path = tmp.path().join("file");
    write_raw_file(&fs, &path, 1, 4_096);
    let missing = tmp.path().join("missing");

    for (what, file, offset, len, kind) in [
        (
            "offset past i64",
            &path,
            u64::MAX,
            1,
            ErrorKind::InvalidInput,
        ),
        ("end past u64", &path, 1, u64::MAX, ErrorKind::InvalidInput),
        ("missing file", &missing, 0, 4_096, ErrorKind::NotFound),
    ] {
        let raw = [
            fs.punch_hole(file, offset, len).map(|()| None),
            fs.extent_is_hole(file, offset, len),
            fs.extent_contains_hole(file, offset, len),
        ];
        let std = [
            std_fs.punch_hole(file, offset, len).map(|()| None),
            std_fs.extent_is_hole(file, offset, len),
            std_fs.extent_contains_hole(file, offset, len),
        ];
        for (raw, std) in raw.iter().zip(&std) {
            assert_eq!(raw.as_ref().map_err(|e| e.kind()), Err(&kind), "{what}");
            assert_eq!(std.as_ref().map_err(|e| e.kind()), Err(&kind), "{what}");
        }
    }
}

/// A path the kernel cannot be handed (an interior NUL) reports nothing it
/// cannot prove: the conservative capabilities and no volume, as through
/// `StdFs`; the operations on it fail.
#[cfg(feature = "std")]
#[test]
fn a_path_with_a_nul_proves_no_capability_and_no_volume() {
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let path = std::path::Path::new("bad\0path");
    assert_eq!(fs.capabilities(path), crate::fs::StdFs.capabilities(path));
    assert_eq!(fs.capabilities(path), crate::fs::FsCapabilities::default());
    assert_eq!(fs.volume_id(path), None);
    assert_eq!(crate::fs::StdFs.volume_id(path), None);
    assert!(fs.allocated_size(path).is_err());
    assert!(fs.hard_link_count(path).is_err());
    assert!(fs.hard_link(path, std::path::Path::new("other")).is_err());
}

/// A clone of a file that does not exist is not found, and turning
/// copy-on-write off fails for a path no writable file sits at, as through
/// `StdFs`; neither leaves a target behind.
#[cfg(all(
    feature = "std",
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    )
))]
#[test]
fn raw_clone_and_copy_on_write_refuse_what_the_std_backend_refuses() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs = IoUringRawFs::new(8).expect("fs setup");
    let (missing, clone) = (tmp.path().join("missing"), tmp.path().join("clone"));
    assert_eq!(
        fs.reflink_file(&missing, &clone)
            .expect_err("no source")
            .kind(),
        ErrorKind::NotFound
    );
    assert!(!clone.exists(), "no target is left behind");
    assert_eq!(
        crate::fs::StdFs
            .reflink_file(&missing, &clone)
            .expect_err("no source")
            .kind(),
        ErrorKind::NotFound
    );
    // A directory does not open for writing.
    assert!(fs.try_disable_cow(tmp.path()).is_err());
    assert!(crate::fs::StdFs.try_disable_cow(tmp.path()).is_err());
}
