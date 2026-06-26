#![expect(
    clippy::expect_used,
    reason = "test assertions over known-good fixtures; failure surfaces via panic"
)]
#![expect(
    clippy::indexing_slicing,
    reason = "test code indexes fixture buffers with known sizes"
)]

use super::*;
use std::io::{Read, Write};
use std::sync::Arc;
// Shadows #[test] to enable log capture in test output.
use test_log::test;

/// Returns an `IoUringFs`, skipping only if the kernel lacks `io_uring`.
/// Constructor bugs (e.g. broken `RingThread::spawn`) will panic the
/// test instead of silently skipping.
fn try_io_uring() -> Option<IoUringFs> {
    if !is_io_uring_available() {
        eprintln!("skipping: io_uring not supported by kernel");
        return None;
    }
    // Kernel supports io_uring — constructor failures are real bugs.
    Some(IoUringFs::new().expect("io_uring available but IoUringFs::new() failed"))
}

#[test]
fn probe_availability() {
    // Just exercises the probe — result depends on the kernel.
    let available = is_io_uring_available();
    eprintln!("io_uring available: {available}");
}

#[test]
fn create_read_write() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

    let path = dir.path().join("test.txt");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    file.sync_all()?;
    drop(file);

    let opts = FsOpenOptions::new().read(true);
    let mut file = fs.open(&path, &opts)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hello world");

    Ok(())
}

#[test]
fn read_at_pread_semantics() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

    let path = dir.path().join("pread.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    file.sync_data()?;

    let mut buf = [0u8; 5];
    let n = file.read_at(&mut buf, 6)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"world");

    let n = file.read_at(&mut buf, 0)?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");

    Ok(())
}

#[test]
fn directory_operations() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

    let nested = dir.path().join("a").join("b").join("c");
    fs.create_dir_all(&nested)?;
    assert!(fs.exists(&nested)?);

    let file_path = nested.join("data.bin");
    let opts = FsOpenOptions::new().write(true).create_new(true);
    let mut file = fs.open(&file_path, &opts)?;
    file.write_all(b"data")?;
    drop(file);

    let entries: Vec<_> = fs.read_dir(&nested)?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].file_name, "data.bin");

    let meta = fs.metadata(&file_path)?;
    assert!(meta.is_file);
    assert_eq!(meta.len, 4);

    fs.remove_file(&file_path)?;
    assert!(!fs.exists(&file_path)?);

    let top = dir.path().join("a");
    fs.remove_dir_all(&top)?;
    assert!(!fs.exists(&top)?);

    Ok(())
}

#[test]
fn rename() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

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
fn sync_directory() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    fs.sync_directory(dir.path())?;
    Ok(())
}

#[test]
fn file_metadata() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

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
fn file_set_len() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

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
fn lock_exclusive() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;

    let path = dir.path().join("lockfile");
    let opts = FsOpenOptions::new().write(true).create(true);
    let file = fs.open(&path, &opts)?;
    file.lock_exclusive()?;

    Ok(())
}

#[test]
fn truncate_and_append() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("trunc.txt");

    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;
    drop(file);

    let opts = FsOpenOptions::new().write(true).truncate(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hi")?;
    drop(file);

    let meta = fs.metadata(&path)?;
    assert_eq!(meta.len, 2);

    let opts = FsOpenOptions::new().write(true).append(true);
    let mut file = fs.open(&path, &opts)?;
    // Seek to start, then write — append mode must ignore seek and
    // write at EOF regardless of cursor position.
    file.seek(SeekFrom::Start(0))?;
    file.write_all(b"!")?;
    drop(file);

    // Verify append went to EOF (len=3), not to start (which would
    // overwrite "hi" and keep len=2).
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    assert_eq!(buf, "hi!");
    assert_eq!(fs.metadata(&path)?.len, 3);

    Ok(())
}

#[test]
fn seek_operations() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("seek.bin");

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"hello world")?;

    // Seek to start and re-read
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; 5];
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"hello");

    // Seek from current (+1 to skip space)
    file.seek(SeekFrom::Current(1))?;
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"world");

    // Seek from end
    let pos = file.seek(SeekFrom::End(-5))?;
    assert_eq!(pos, 6);

    Ok(())
}

#[test]
fn concurrent_read_at() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("concurrent.bin");

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    // Write 1000 bytes: each byte = (offset % 256)
    #[expect(clippy::cast_possible_truncation, reason = "% 256 guarantees 0..=255")]
    let data: Vec<u8> = (0u32..1000).map(|i| (i % 256) as u8).collect();
    file.write_all(&data)?;
    file.sync_all()?;

    let file = Arc::new(file);
    let mut handles = Vec::new();

    for chunk_start in (0..1000).step_by(100) {
        let file = Arc::clone(&file);
        handles.push(thread::spawn(move || -> io::Result<()> {
            let mut buf = [0u8; 100];
            let n = file.read_at(&mut buf, chunk_start as u64)?;
            assert_eq!(n, 100);
            for (i, &byte) in buf.iter().enumerate() {
                #[expect(clippy::cast_possible_truncation, reason = "% 256 guarantees 0..=255")]
                let expected = ((chunk_start + i) % 256) as u8;
                assert_eq!(byte, expected);
            }
            Ok(())
        }));
    }

    for h in handles {
        match h.join() {
            Ok(result) => result?,
            Err(_) => return Err(io::Error::other("thread panicked")),
        }
    }

    Ok(())
}

#[test]
fn metadata_directory() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let meta = fs.metadata(dir.path())?;
    assert!(meta.is_dir);
    assert!(!meta.is_file);

    Ok(())
}

#[test]
fn object_safety() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let fs: Arc<dyn Fs> = Arc::new(fs);
    let dir = tempfile::tempdir()?;
    let bogus = dir.path().join("nonexistent");
    assert!(!fs.exists(&bogus)?);
    Ok(())
}

#[test]
fn empty_buffer_returns_zero() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("empty_buf.bin");

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"data")?;

    // read_at with empty buffer
    let n = file.read_at(&mut [], 0)?;
    assert_eq!(n, 0);

    // Read::read with empty buffer
    let n = file.read(&mut [])?;
    assert_eq!(n, 0);

    // Write::write with empty buffer
    let n = file.write(&[])?;
    assert_eq!(n, 0);

    // flush is a no-op
    file.flush()?;

    Ok(())
}

#[test]
fn sync_directory_rejects_file() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("not_a_dir.txt");

    let opts = FsOpenOptions::new().write(true).create(true);
    fs.open(&path, &opts)?;

    match fs.sync_directory(&path) {
        Ok(()) => panic!("sync_directory on a file should fail"),
        // sync_directory is an `Fs` method → returns `crate::io::Result`.
        Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput),
    }

    Ok(())
}

#[test]
fn seek_overflow_returns_error() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("seek_overflow.bin");

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"data")?;

    // Seek to near u64::MAX, then seek forward — should overflow.
    file.seek(SeekFrom::Start(u64::MAX - 1))?;
    match file.seek(SeekFrom::Current(2)) {
        Ok(_) => panic!("seek past u64::MAX should fail"),
        Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
    }

    // SeekFrom::Current negative past zero — should underflow.
    file.seek(SeekFrom::Start(0))?;
    match file.seek(SeekFrom::Current(-1)) {
        Ok(_) => panic!("seek before zero should fail"),
        Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
    }

    // SeekFrom::End negative past zero — should underflow.
    match file.seek(SeekFrom::End(-100)) {
        Ok(_) => panic!("seek before zero should fail"),
        Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
    }

    Ok(())
}

#[test]
fn debug_impl() {
    let Some(fs) = try_io_uring() else {
        return;
    };
    let debug = format!("{fs:?}");
    assert!(debug.contains("IoUringFs"));
}

#[test]
fn with_ring_size() -> io::Result<()> {
    if !is_io_uring_available() {
        eprintln!("skipping: io_uring not supported by kernel");
        return Ok(());
    }
    let fs =
        IoUringFs::with_ring_size(64).expect("io_uring available but with_ring_size(64) failed");
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("ring64.bin");
    let opts = FsOpenOptions::new().write(true).create(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"ok")?;
    file.sync_all()?;
    assert_eq!(fs.metadata(&path)?.len, 2);
    Ok(())
}

#[test]
fn seek_negative_from_current() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("seek_neg.bin");

    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(b"abcdefghij")?;

    // Seek to position 8, then back 3
    file.seek(SeekFrom::Start(8))?;
    let pos = file.seek(SeekFrom::Current(-3))?;
    assert_eq!(pos, 5);

    let mut buf = [0u8; 5];
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"fghij");

    Ok(())
}

#[test]
fn clone_shares_ring() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let fs2 = fs.clone();
    let dir = tempfile::tempdir()?;

    // Both clones should work with the same ring thread.
    let p1 = dir.path().join("a.txt");
    let p2 = dir.path().join("b.txt");
    let opts = FsOpenOptions::new().write(true).create(true);

    let mut f1 = fs.open(&p1, &opts)?;
    let mut f2 = fs2.open(&p2, &opts)?;
    f1.write_all(b"one")?;
    f2.write_all(b"two")?;
    f1.sync_all()?;
    f2.sync_all()?;

    assert_eq!(fs.metadata(&p1)?.len, 3);
    assert_eq!(fs2.metadata(&p2)?.len, 3);

    Ok(())
}

#[test]
fn available_space_reports_plausible_free_bytes() -> io::Result<()> {
    // The cold-path free-space probe delegates to the shared statvfs helper:
    // the filesystem backing the tempdir must report a plausible, non-zero
    // figure below the unbounded sentinel.
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let free = fs.available_space(dir.path())?;
    assert!(
        free > 0,
        "a writable tempdir filesystem must report free space"
    );
    assert!(
        free < u64::MAX,
        "a real probe must not return the unbounded sentinel"
    );
    Ok(())
}

#[test]
fn read_many_fills_every_region() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("read_many.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    let data: Vec<u8> = (0..=255u8).collect();
    file.write_all(&data)?;
    file.sync_all()?;

    // Disjoint regions in non-file order plus an EMPTY region (the submit loop
    // skips it) must each be filled by the one batched submission.
    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 8];
    let mut empty: [u8; 0] = [];
    let mut b2 = [0u8; 1];
    let mut regions: Vec<(u64, &mut [u8])> = vec![
        (10, &mut b0[..]),
        (200, &mut b1[..]),
        (50, &mut empty[..]),
        (0, &mut b2[..]),
    ];
    file.read_many(&mut regions)?;
    drop(regions);

    assert_eq!(&b0[..], &data[10..14]);
    assert_eq!(&b1[..], &data[200..208]);
    assert_eq!(b2[0], data[0]);
    Ok(())
}

#[test]
fn read_blocks_batched_across_files_via_ring() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut f0 = fs.open(&dir.path().join("a.bin"), &opts)?;
    f0.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    f0.sync_all()?;
    let mut f1 = fs.open(&dir.path().join("b.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    f1.write_all(&rev)?;
    f1.sync_all()?;

    // Both handles back onto the SAME shared ring, so reads from two files
    // submit in one batch (submit_reads_multi, per-request fd).
    assert!(f0.backing_fd().is_some(), "io_uring file exposes its fd");
    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
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
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }
    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    Ok(())
}

#[test]
fn read_many_short_read_at_eof_errors() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("short_many.bin");
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&path, &opts)?;
    file.write_all(&[0u8; 10])?;
    file.sync_all()?;

    // First region runs past EOF (a fixed-size short read = UnexpectedEof, not
    // EOF); the second is in range. The first failing must NOT short-circuit the
    // drain: every later op is still recv'd so its in-flight kernel write
    // completes before the buffers are freed. The call surfaces the first error.
    let mut short = [0u8; 32];
    let mut ok = [0u8; 4];
    let mut regions: Vec<(u64, &mut [u8])> = vec![(0, &mut short[..]), (0, &mut ok[..])];
    match file.read_many(&mut regions) {
        Ok(()) => panic!("read past EOF must fail, not report a short read as success"),
        Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
    }
    Ok(())
}

#[test]
fn read_blocks_batched_short_read_at_eof_errors() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&dir.path().join("short_block.bin"), &opts)?;
    file.write_all(&[0u8; 10])?;
    file.sync_all()?;

    // First block runs past EOF; the second is in range. The failing block must
    // not short-circuit the drain (every later op is recv'd before return).
    let mut short = [0u8; 64];
    let mut ok = [0u8; 4];
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: file.as_ref(),
                offset: 0,
                buf: &mut short,
            },
            crate::fs::BlockRead {
                file: file.as_ref(),
                offset: 0,
                buf: &mut ok,
            },
        ];
        match fs.read_blocks_batched(&mut reqs) {
            Ok(()) => panic!("read past EOF must fail, not report a short read as success"),
            Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
        }
    }
    Ok(())
}

#[test]
fn read_blocks_batched_fallback_short_read_errors() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&[0u8; 64])?;
    uring_file.sync_all()?;

    // A StdFs handle (no fd for the ring) forces the whole batch onto the serial
    // read_at fallback; its block runs past EOF, so the fallback's fixed-size
    // short read is reported as UnexpectedEof, not a silent partial fill.
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[0u8; 10])?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 8];
    let mut b1 = [0u8; 64]; // past EOF on the std file
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: &mut b0,
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 0,
                buf: &mut b1,
            },
        ];
        match fs.read_blocks_batched(&mut reqs) {
            Ok(()) => panic!("a short read in the serial fallback must fail"),
            Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
        }
    }
    Ok(())
}

#[test]
fn read_blocks_batched_falls_back_for_non_uring_file() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    // A StdFs handle has no fd for the ring (backing_fd None), so mixing it into
    // the batch forces the whole batch onto the serial read_at fallback.
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    std_file.write_all(&rev)?;
    std_file.sync_all()?;
    assert_eq!(std_file.backing_fd(), None);

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: &mut b0,
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 20,
                buf: &mut b1,
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }
    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    Ok(())
}

#[test]
fn volume_id_matches_the_kernel_mount() -> io::Result<()> {
    // Free space is a property of the mount, not the I/O submission path, so
    // the uring backend reports the same volume id as `StdFs` for a path —
    // letting the space gate treat a uring data dir and a `StdFs` blob dir on
    // the same mount as one free-space pool.
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    assert_eq!(
        fs.volume_id(dir.path()),
        crate::fs::StdFs.volume_id(dir.path()),
        "uring and std agree on the mount backing a path"
    );
    assert!(fs.volume_id(dir.path()).is_some(), "a real mount has an id");
    Ok(())
}
