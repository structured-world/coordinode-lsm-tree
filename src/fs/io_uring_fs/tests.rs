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
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: f1.as_ref(),
                offset: 20,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }
    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    Ok(())
}

/// A request whose destination arrives partly filled must resume at
/// `offset + filled` on the ring path too: the SQE's pointer and length
/// already describe the unfilled suffix, so submitting the original offset
/// would duplicate the block's first bytes into that suffix.
#[test]
fn read_blocks_batched_partly_filled_request_resumes_via_ring() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut file = fs.open(&dir.path().join("resume.bin"), &opts)?;
    file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    file.sync_all()?;
    assert!(file.backing_fd().is_some(), "io_uring file exposes its fd");

    // Block = bytes 10..18; the first 3 are already in the destination, so the
    // ring owes bytes 13..18 into the suffix.
    let mut buf = [0u8; 8];
    {
        let mut dst = crate::fs::BlockBuf::new(&mut buf);
        dst.append(&[10, 11, 12]);
        let mut reqs = vec![crate::fs::BlockRead {
            file: file.as_ref(),
            offset: 10,
            buf: dst,
        }];
        fs.read_blocks_batched(&mut reqs)?;
        assert!(reqs[0].buf.is_full(), "the request is completed, not short");
    }
    assert_eq!(buf, [10, 11, 12, 13, 14, 15, 16, 17]);
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
    file.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    file.sync_all()?;

    // First region runs past EOF (a fixed-size short read = UnexpectedEof, not
    // EOF); the second is in range. The first failing must NOT short-circuit the
    // drain: every later op is still recv'd so its in-flight kernel write
    // completes before the buffers are freed. The call surfaces the first error.
    let mut short = [0u8; 32];
    let mut ok = [0xAAu8; 4]; // sentinel: the drained read must overwrite it
    let mut regions: Vec<(u64, &mut [u8])> = vec![(0, &mut short[..]), (4, &mut ok[..])];
    match file.read_many(&mut regions) {
        Ok(()) => panic!("read past EOF must fail, not report a short read as success"),
        Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
    }
    // The in-range op was drained to completion despite the earlier short read:
    // its buffer holds the on-disk bytes, not the sentinel.
    assert_eq!(ok, [5, 6, 7, 8]);
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
    file.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    file.sync_all()?;

    // First block runs past EOF; the second is in range. The failing block must
    // not short-circuit the drain (every later op is recv'd before return).
    let mut short = [0u8; 64];
    let mut ok = [0xAAu8; 4]; // sentinel: the drained read must overwrite it
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut short),
            },
            crate::fs::BlockRead {
                file: file.as_ref(),
                offset: 4,
                buf: crate::fs::BlockBuf::new(&mut ok),
            },
        ];
        match fs.read_blocks_batched(&mut reqs) {
            Ok(()) => panic!("read past EOF must fail, not report a short read as success"),
            Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
        }
    }
    // The in-range op was drained to completion: its buffer holds on-disk bytes.
    assert_eq!(ok, [5, 6, 7, 8]);
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
    uring_file.write_all(&(1..=64u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    // A StdFs handle (no fd for the ring) forces the whole batch onto the serial
    // read_at fallback; its block runs past EOF, so the fallback's fixed-size
    // short read is reported as UnexpectedEof, not a silent partial fill.
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[0u8; 10])?;
    std_file.sync_all()?;

    let mut b0 = [0xAAu8; 8]; // sentinel: the in-range fallback read must fill it
    let mut b1 = [0u8; 64]; // past EOF on the std file
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        match fs.read_blocks_batched(&mut reqs) {
            Ok(()) => panic!("a short read in the serial fallback must fail"),
            Err(err) => assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof),
        }
    }
    // The in-range block was read by the fallback before the short one failed.
    assert_eq!(b0, [1, 2, 3, 4, 5, 6, 7, 8]);
    Ok(())
}

#[test]
fn read_blocks_batched_serves_a_mixed_batch_without_degrading_the_ring_group() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;
    // Wrapped so the test can see whether this request was submitted to the
    // ring or read through the serial `read_at` fallback.
    let counted = CountingFile::new(uring_file);

    // A StdFs handle has no fd for the ring (backing_fd None), so it cannot be
    // submitted and is read serially — but only it.
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
                file: &counted,
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 20,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }
    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    assert_eq!(
        counted.read_at_calls(),
        0,
        "the descriptor-bearing request went to the ring; one handle without a \
         descriptor must not drag the rest onto the serial path",
    );
    Ok(())
}

/// The split must not reorder what each request reads: every destination holds
/// the bytes ITS `(file, offset)` names, whichever group served it.
#[test]
fn read_blocks_batched_keeps_each_destination_with_its_own_request() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    std_file.write_all(&rev)?;
    std_file.sync_all()?;

    // Interleaved, so a split that preserved only group-internal order would
    // still be caught. Offsets stay within the 256-byte fixtures, and `u8`
    // keeps the expected bytes derivable without a narrowing cast.
    let mut bufs = [[0u8; 2]; 6];
    let offsets: [u8; 6] = [0, 10, 20, 30, 40, 50];
    {
        let mut reqs: Vec<crate::fs::BlockRead<'_>> = Vec::new();
        for (i, (buf, offset)) in bufs.iter_mut().zip(offsets).enumerate() {
            reqs.push(crate::fs::BlockRead {
                file: if i % 2 == 0 {
                    uring_file.as_ref()
                } else {
                    std_file.as_ref()
                },
                offset: u64::from(offset),
                buf: crate::fs::BlockBuf::new(&mut buf[..]),
            });
        }
        fs.read_blocks_batched(&mut reqs)?;
    }
    for (i, (buf, offset)) in bufs.iter().zip(offsets).enumerate() {
        let at = usize::from(offset);
        // The uring fixture holds byte value == its offset; the std fixture
        // holds the reverse.
        let expected: [u8; 2] = if i % 2 == 0 {
            [offset, offset + 1]
        } else {
            [rev[at], rev[at + 1]]
        };
        assert_eq!(*buf, expected, "request {i} was served the wrong bytes");
    }
    Ok(())
}

/// A batch where NOTHING can be submitted still reads correctly: the ring group
/// is empty and every request takes the serial path.
#[test]
fn read_blocks_batched_with_no_submittable_request_reads_serially() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 100,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
    }
    assert_eq!(b0, [0, 1, 2, 3]);
    assert_eq!(b1, [100, 101, 102, 103]);
    Ok(())
}

/// A short read in the SERIAL half of a mixed batch is still the documented
/// `UnexpectedEof`, and the ring half is still filled — the split must not turn
/// one group's failure into silence about the other.
#[test]
fn read_blocks_batched_reports_a_short_serial_read_in_a_mixed_batch() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[1, 2, 3, 4])?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 8]; // past EOF of the 4-byte file
    let err = {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("a short read on a fixed-size block is an error")
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof, "{err}");
    assert_eq!(b0, [10, 11, 12, 13], "the ring group was served first");
    Ok(())
}

/// A short read in the RING half of a mixed batch fails the whole call, and
/// the serial half is left unread: the ring group is submitted first and its
/// failure is surfaced before anything else runs.
#[test]
fn read_blocks_batched_reports_a_short_ring_read_in_a_mixed_batch() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&[1, 2, 3, 4])?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 8]; // past EOF of the 4-byte uring file
    let mut b1 = [0u8; 4];
    let err = {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("a short read on a fixed-size block is an error")
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof, "{err}");
    Ok(())
}

/// A destination that arrives partly filled owns the block's first bytes
/// already, so the read covers the suffix from `offset + filled` — on both
/// sides of the split.
#[test]
fn read_blocks_batched_resumes_partly_filled_destinations_in_a_mixed_batch() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    std_file.write_all(&rev)?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    {
        let mut buf0 = crate::fs::BlockBuf::new(&mut b0);
        let mut buf1 = crate::fs::BlockBuf::new(&mut b1);
        // Two bytes of each block are already owned by the caller.
        assert_eq!(buf0.append(&[0xAA, 0xBB]), 2);
        assert_eq!(buf1.append(&[0xCC, 0xDD]), 2);
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: buf0,
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 20,
                buf: buf1,
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
        assert!(reqs.iter().all(|r| r.buf.is_full()));
    }
    // The suffix starts two bytes into each block, and the prefix is untouched.
    assert_eq!(b0, [0xAA, 0xBB, 12, 13]);
    assert_eq!(b1, [0xCC, 0xDD, rev[22], rev[23]]);
    Ok(())
}

/// An empty request is a no-op wherever it lands, including in the middle of a
/// split batch: the ring skips it and the serial loop reads nothing for it.
#[test]
fn read_blocks_batched_tolerates_an_empty_request_in_a_mixed_batch() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    let rev: Vec<u8> = (0..=255u8).rev().collect();
    std_file.write_all(&rev)?;
    std_file.sync_all()?;

    let mut empty_uring: [u8; 0] = [];
    let mut empty_std: [u8; 0] = [];
    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut empty_uring),
            },
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut empty_std),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 20,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)?;
        assert!(
            reqs.iter().all(|r| r.buf.is_full()),
            "an empty destination is trivially full, and the rest were filled",
        );
    }
    assert_eq!(b0, [10, 11, 12, 13]);
    assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
    Ok(())
}

/// When BOTH halves of a split batch fail, the error returned is the one whose
/// request came first in the CALLER's order — the contract is the first failing
/// block, and splitting must not re-order which failure wins. The serial
/// request is placed first here, so its error has to beat the ring's.
#[test]
fn read_blocks_batched_reports_the_earliest_failure_when_both_halves_fail() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    // Both files are 4 bytes, so both requests below read past EOF.
    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&[1, 2, 3, 4])?;
    uring_file.sync_all()?;
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[1, 2, 3, 4])?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 8];
    let mut b1 = [0u8; 8];
    let err = {
        let mut serial_first = crate::fs::BlockBuf::new(&mut b0);
        // An overflowing resume offset, which is `InvalidInput` — distinct from
        // the ring half's `UnexpectedEof`, so the verdict says which one won.
        assert_eq!(serial_first.append(&[0xAA, 0xBB]), 2);
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: u64::MAX - 1,
                buf: serial_first,
            },
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("both halves fail, so the call fails")
    };
    assert_eq!(
        err.kind(),
        crate::io::ErrorKind::InvalidInput,
        "the request at index 0 is the serial one, so its error wins: {err}",
    );
    Ok(())
}

/// The mirror of the case above: when the RING half holds the earlier request,
/// its error is the one reported.
#[test]
fn read_blocks_batched_prefers_the_ring_failure_when_it_came_first() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&[1, 2, 3, 4])?;
    uring_file.sync_all()?;
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[1, 2, 3, 4])?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 8];
    let mut b1 = [0u8; 8];
    let err = {
        let mut serial_second = crate::fs::BlockBuf::new(&mut b1);
        assert_eq!(serial_second.append(&[0xAA, 0xBB]), 2);
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: u64::MAX - 1,
                buf: serial_second,
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("both halves fail, so the call fails")
    };
    assert_eq!(
        err.kind(),
        crate::io::ErrorKind::UnexpectedEof,
        "the request at index 0 is the ring one, so its short read wins: {err}",
    );
    Ok(())
}

/// The resume-offset overflow guard covers the RING half too: a submittable
/// request whose partly filled destination would push its offset past `u64`
/// is refused before anything is submitted, not wrapped around into a read of
/// some other part of the file.
#[test]
fn read_blocks_batched_rejects_an_overflowing_resume_offset_on_the_ring_side() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let mut buf = [0u8; 4];
    let err = {
        let mut dst = crate::fs::BlockBuf::new(&mut buf);
        assert_eq!(dst.append(&[0xAA, 0xBB]), 2);
        let mut reqs = vec![crate::fs::BlockRead {
            file: uring_file.as_ref(),
            offset: u64::MAX - 1,
            buf: dst,
        }];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("an overflowing resume offset must be refused")
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput, "{err}");
    Ok(())
}

/// A serial read that FAILS outright (as opposed to coming up short) is
/// reported as itself: the backend's own error, not a substitute.
#[test]
fn read_blocks_batched_surfaces_a_serial_read_error_verbatim() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    // A descriptor-less handle whose reads are made to fail.
    let faulty = crate::fs::FaultFs::new(crate::fs::StdFs);
    let mut std_file = faulty.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    std_file.sync_all()?;
    assert_eq!(std_file.backing_fd(), None);
    faulty.injector().arm(crate::fs::FaultRule::new(
        crate::fs::FaultOp::ReadAt,
        crate::fs::Fault::Error(crate::io::ErrorKind::PermissionDenied),
    ));

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    let err = {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: 20,
                buf: crate::fs::BlockBuf::new(&mut b1),
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("the serial half's read fails")
    };
    assert_eq!(
        err.kind(),
        crate::io::ErrorKind::PermissionDenied,
        "the backend's own error is what the caller sees: {err}",
    );
    assert_eq!(b0, [10, 11, 12, 13], "the ring half still ran");
    Ok(())
}

/// A ring request that SUCCEEDS must not lend its index to a later one that
/// fails. With a good ring read at 0, a failing serial read at 1 and a failing
/// ring read at 2, the winner is the serial failure at index 1 — attributing
/// the ring's failure to the group's first request would wrongly report index 0
/// and hand back the wrong error.
#[test]
fn read_blocks_batched_blames_the_ring_request_that_failed_not_its_group() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;
    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&[1, 2, 3, 4])?;
    std_file.sync_all()?;

    let mut good = [0u8; 4];
    let mut serial_bad = [0u8; 4];
    let mut ring_bad = [0u8; 8];
    let err = {
        let mut serial_buf = crate::fs::BlockBuf::new(&mut serial_bad);
        // Overflowing resume offset → InvalidInput, at index 1.
        assert_eq!(serial_buf.append(&[0xAA, 0xBB]), 2);
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut good),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: u64::MAX - 1,
                buf: serial_buf,
            },
            crate::fs::BlockRead {
                // Reads past the 256-byte fixture's end → UnexpectedEof, at 2.
                file: uring_file.as_ref(),
                offset: 252,
                buf: crate::fs::BlockBuf::new(&mut ring_bad),
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("two of the three requests fail")
    };
    assert_eq!(
        err.kind(),
        crate::io::ErrorKind::InvalidInput,
        "index 1 fails before index 2, so the serial error wins: {err}",
    );
    Ok(())
}

/// An offset that would overflow while resuming a partly filled destination is
/// rejected, not wrapped into a read somewhere else in the file. The check
/// lives in the serial half of the split, so a mixed batch has to reach it.
#[test]
fn read_blocks_batched_rejects_an_overflowing_resume_offset() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let opts = FsOpenOptions::new().write(true).create(true).read(true);

    let mut uring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
    uring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    uring_file.sync_all()?;

    let std_fs = crate::fs::StdFs;
    let mut std_file = std_fs.open(&dir.path().join("s.bin"), &opts)?;
    std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
    std_file.sync_all()?;

    let mut b0 = [0u8; 4];
    let mut b1 = [0u8; 4];
    let err = {
        let mut buf1 = crate::fs::BlockBuf::new(&mut b1);
        // Two bytes already owned, so the resume offset is `u64::MAX - 1 + 2`.
        assert_eq!(buf1.append(&[0xAA, 0xBB]), 2);
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: uring_file.as_ref(),
                offset: 10,
                buf: crate::fs::BlockBuf::new(&mut b0),
            },
            crate::fs::BlockRead {
                file: std_file.as_ref(),
                offset: u64::MAX - 1,
                buf: buf1,
            },
        ];
        fs.read_blocks_batched(&mut reqs)
            .expect_err("an overflowing resume offset must be refused")
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput, "{err}");
    Ok(())
}

/// Wraps a file handle, passing its descriptor through so the ring still
/// accepts it, and counts the serial `read_at` calls made against it. A count
/// above zero says the request took the fallback path.
struct CountingFile {
    inner: Box<dyn crate::fs::FsFile>,
    read_at_calls: std::sync::atomic::AtomicUsize,
}

impl CountingFile {
    fn new(inner: Box<dyn crate::fs::FsFile>) -> Self {
        Self {
            inner,
            read_at_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn read_at_calls(&self) -> usize {
        self.read_at_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl io::Read for CountingFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl io::Seek for CountingFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl crate::fs::FsFile for CountingFile {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.inner.sync_all()
    }

    fn sync_data(&self) -> crate::io::Result<()> {
        self.inner.sync_data()
    }

    fn metadata(&self) -> crate::io::Result<crate::fs::FsMetadata> {
        self.inner.metadata()
    }

    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        self.read_at_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read_at(buf, offset)
    }

    fn backing_fd(&self) -> Option<i32> {
        self.inner.backing_fd()
    }

    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.inner.lock_exclusive()
    }
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

/// Two files of `len` bytes each, byte `i` of file `f` being `(i + f) % 251`.
fn two_files(fs: &IoUringFs, dir: &Path, len: usize) -> io::Result<Vec<Box<dyn FsFile>>> {
    let opts = FsOpenOptions::new().write(true).create(true).read(true);
    let mut files = Vec::new();
    for f in 0..2usize {
        let mut file = fs.open(&dir.join(format!("f{f}.bin")), &opts)?;
        let bytes: Vec<u8> = (0..len)
            .map(|i| u8::try_from((i + f) % 251).expect("below 251"))
            .collect();
        file.write_all(&bytes)?;
        file.sync_all()?;
        files.push(file);
    }
    Ok(files)
}

/// What a caller spends per batch before the kernel sees it is one completion
/// channel and one acquisition of the submission lock, however many reads the
/// batch holds.
#[test]
fn a_batched_read_takes_one_channel_and_one_lock_per_batch() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files = two_files(&fs, dir.path(), 64 * 16)?;
    let mut buffers = vec![[0u8; 16]; 64];
    let mut reqs: Vec<_> = buffers
        .iter_mut()
        .enumerate()
        .map(|(i, buf)| crate::fs::BlockRead {
            file: files[i % 2].as_ref(),
            offset: (i * 16) as u64,
            buf: crate::fs::BlockBuf::new(buf),
        })
        .collect();

    let counts = &fs.inner.counts;
    let channels = counts.channels.load(core::sync::atomic::Ordering::Relaxed);
    let locks = counts.locks.load(core::sync::atomic::Ordering::Relaxed);
    fs.read_blocks_batched(&mut reqs)?;
    assert_eq!(
        counts.channels.load(core::sync::atomic::Ordering::Relaxed) - channels,
        1
    );
    assert_eq!(
        counts.locks.load(core::sync::atomic::Ordering::Relaxed) - locks,
        1
    );
    drop(reqs);
    for (i, buf) in buffers.iter().enumerate() {
        let expected: Vec<u8> = (0..16)
            .map(|b| u8::try_from((i * 16 + b + i % 2) % 251).expect("below 251"))
            .collect();
        assert_eq!(buf.as_slice(), expected, "block {i}");
    }
    Ok(())
}

/// `drain_batch` over completions fed in the given order: `(position, result)`.
fn drain_in(
    expected: &[usize],
    order: &[(usize, i32)],
) -> (Result<(), (usize, io::Error)>, Vec<usize>) {
    let mut feed = order.iter().copied();
    let mut handed = Vec::new();
    let verdict = drain_batch(expected, || feed.next(), "short", |p| handed.push(p));
    (verdict, handed)
}

/// Which failure a batch reports is a rule over the requests, not a race: the
/// lowest position, whatever order its completions arrive in.
#[test]
fn a_failed_batch_reports_the_lowest_failure_in_any_completion_order() {
    // Position 1 fails with EIO, position 3 reads short, position 4 fails with
    // ENOSPC; 0 and 2 succeed.
    let expected = [8, 8, 8, 8, 8];
    let events = [(0, 8), (1, -5), (2, 8), (3, 2), (4, -28)];
    let forward: Vec<_> = events.to_vec();
    let reverse: Vec<_> = events.iter().rev().copied().collect();
    let shuffled = vec![events[3], events[0], events[4], events[2], events[1]];

    for order in [forward, reverse, shuffled] {
        let (verdict, mut handed) = drain_in(&expected, &order);
        let Err((position, error)) = verdict else {
            panic!("a batch with failures must fail ({order:?})");
        };
        assert_eq!(position, 1, "{order:?}");
        assert_eq!(error.raw_os_error(), Some(5), "{order:?}");
        handed.sort_unstable();
        assert_eq!(
            handed,
            [0, 2],
            "only full reads are handed over ({order:?})"
        );
    }
}

/// A read that never completes, because the ring went away, is a broken pipe
/// at its position; a failure at a lower position still wins over it.
#[test]
fn a_batch_cut_short_reports_the_missing_read() {
    let (verdict, _) = drain_in(&[4, 4, 4], &[(0, 4), (2, 4)]);
    let Err((position, error)) = verdict else {
        panic!("a read that never completed must fail the batch");
    };
    assert_eq!(position, 1);
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

    let (verdict, _) = drain_in(&[4, 4, 4], &[(2, 4), (0, -5)]);
    assert!(matches!(verdict, Err((0, _))));
}

/// A finished read is handed over before later completions are even looked
/// at: the caller works on it while the rest are still in flight.
#[test]
fn a_finished_read_is_handed_over_before_later_completions_arrive() {
    let log = core::cell::RefCell::new(Vec::new());
    let mut feed = [(2, 4), (0, 4), (1, 4)].into_iter();
    let verdict = drain_batch(
        &[4, 4, 4],
        || {
            let next = feed.next();
            if let Some((position, _)) = next {
                log.borrow_mut().push(format!("complete {position}"));
            }
            next
        },
        "short",
        |position| log.borrow_mut().push(format!("hand {position}")),
    );
    assert!(verdict.is_ok());
    assert_eq!(
        log.into_inner(),
        [
            "complete 2",
            "hand 2",
            "complete 0",
            "hand 0",
            "complete 1",
            "hand 1"
        ]
    );
}

/// Handing requests over as they complete returns exactly what the ordered
/// path returns: the same requests, filled with the same bytes.
#[test]
fn read_blocks_batched_each_returns_what_the_ordered_path_returns() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files = two_files(&fs, dir.path(), 32 * 64)?;
    let read = |each: bool| -> io::Result<(Vec<[u8; 64]>, Vec<usize>)> {
        let mut buffers = vec![[0u8; 64]; 32];
        let mut handed = Vec::new();
        {
            let mut reqs: Vec<_> = buffers
                .iter_mut()
                .enumerate()
                .map(|(i, buf)| crate::fs::BlockRead {
                    file: files[i % 2].as_ref(),
                    offset: (i * 64) as u64,
                    buf: crate::fs::BlockBuf::new(buf),
                })
                .collect();
            if each {
                fs.read_blocks_batched_each(&mut reqs, &mut |i, req| {
                    assert!(req.buf.is_full(), "request {i} handed over unfilled");
                    handed.push(i);
                })?;
            } else {
                fs.read_blocks_batched(&mut reqs)?;
            }
        }
        handed.sort_unstable();
        Ok((buffers, handed))
    };
    let (ordered, _) = read(false)?;
    let (each, handed) = read(true)?;
    assert_eq!(ordered, each);
    assert_eq!(handed, (0..32).collect::<Vec<_>>());
    Ok(())
}

/// A batch mixing reads that fail with reads that succeed, run many times:
/// every successful read is filled and handed over, none that failed is, and
/// the failure reported is always the lowest one. The buffers of reads still
/// in flight when an earlier one fails are written before the call returns.
#[test]
fn a_mixed_batch_drains_every_read_and_reports_the_lowest_failure() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files = two_files(&fs, dir.path(), 16 * 16)?;
    for _ in 0..200 {
        // Every third request reads past the end of its file.
        let mut buffers = vec![[0xAAu8; 16]; 24];
        let mut handed = Vec::new();
        let verdict = {
            let mut reqs: Vec<_> = buffers
                .iter_mut()
                .enumerate()
                .map(|(i, buf)| crate::fs::BlockRead {
                    file: files[i % 2].as_ref(),
                    offset: if i % 3 == 1 {
                        1 << 20
                    } else {
                        (i % 16 * 16) as u64
                    },
                    buf: crate::fs::BlockBuf::new(buf),
                })
                .collect();
            fs.read_blocks_batched_each(&mut reqs, &mut |i, _| handed.push(i))
        };
        let Err(error) = verdict else {
            panic!("reads past the end must fail the batch");
        };
        assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
        handed.sort_unstable();
        let succeeded: Vec<usize> = (0..24).filter(|i| i % 3 != 1).collect();
        assert_eq!(handed, succeeded);
        for &i in &succeeded {
            assert_ne!(buffers[i], [0xAAu8; 16], "read {i} was drained and written");
        }
    }
    Ok(())
}

/// A callback that panics must not cut the drain short: the reads still in
/// flight write into buffers the unwind would free, so every completion is
/// received before the panic carries on.
#[test]
fn a_panicking_callback_still_drains_every_completion() {
    let received = core::cell::Cell::new(0usize);
    let mut feed = [(0, 4), (1, 4), (2, 4)].into_iter();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drain_batch(
            &[4, 4, 4],
            || {
                let next = feed.next();
                if next.is_some() {
                    received.set(received.get() + 1);
                }
                next
            },
            "short",
            |_| panic!("the caller's work on a finished read failed"),
        )
    }));
    assert!(unwound.is_err(), "the callback's panic carries on");
    assert_eq!(received.get(), 3, "every completion was received first");
}

/// An empty request is complete without a read, so it is handed over like
/// every other finished one, as the default and the serial path do.
#[test]
fn an_empty_request_in_a_batch_is_handed_over() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files = two_files(&fs, dir.path(), 64)?;
    let mut first = [0u8; 16];
    let mut empty = [0u8; 0];
    let mut last = [0u8; 16];
    let mut handed = Vec::new();
    {
        let mut reqs = vec![
            crate::fs::BlockRead {
                file: files[0].as_ref(),
                offset: 0,
                buf: crate::fs::BlockBuf::new(&mut first),
            },
            crate::fs::BlockRead {
                file: files[1].as_ref(),
                offset: 16,
                buf: crate::fs::BlockBuf::new(&mut empty),
            },
            crate::fs::BlockRead {
                file: files[1].as_ref(),
                offset: 32,
                buf: crate::fs::BlockBuf::new(&mut last),
            },
        ];
        fs.read_blocks_batched_each(&mut reqs, &mut |i, _| handed.push(i))?;
    }
    handed.sort_unstable();
    assert_eq!(handed, [0, 1, 2]);
    Ok(())
}

/// The bytes a [`two_files`] file `f` holds at `offset`, `len` of them.
fn expected_bytes(f: usize, offset: usize, len: usize) -> Vec<u8> {
    (offset..offset + len)
        .map(|i| u8::try_from((i + f) % 251).expect("below 251"))
        .collect()
}

/// Reads submitted to the queue across two files come back with their tags
/// and bytes, and reads submitted after a wait, while earlier ones may still
/// be on the ring, are told apart from them.
#[test]
fn a_read_queue_hands_back_reads_of_several_submissions() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 64 * 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = fs.read_queue();
    let mut got: Vec<(usize, Vec<u8>)> = Vec::new();
    for round in 0..4usize {
        for i in 0..16usize {
            let tag = round * 16 + i;
            queue.submit(QueuedRead {
                tag,
                file: Arc::clone(&files[tag % 2]),
                offset: (tag * 64) as u64,
                buf: vec![0; 64],
            });
        }
        // Issues this round's reads and takes whatever has come back.
        queue.wait(0, &mut |done| {
            assert!(done.result.is_ok(), "read {} failed", done.tag);
            got.push((done.tag, done.buf));
        });
    }
    while queue.outstanding() > 0 {
        queue.wait(1, &mut |done| {
            assert!(done.result.is_ok(), "read {} failed", done.tag);
            got.push((done.tag, done.buf));
        });
    }
    got.sort_unstable_by_key(|(tag, _)| *tag);
    let expected: Vec<(usize, Vec<u8>)> = (0..64)
        .map(|tag| (tag, expected_bytes(tag % 2, tag * 64, 64)))
        .collect();
    assert_eq!(got, expected);
    Ok(())
}

/// Counts the wakes a queue gives it.
struct CountWake(core::sync::atomic::AtomicUsize);

impl crate::fs::ReadWake for CountWake {
    fn wake(&self) {
        self.0.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
    }
}

/// A queue given a wake rings it once per read the ring finished, and only
/// once that read is ready: after `n` wakes, a wait that does not block hands
/// over at least `n` reads in all.
#[test]
fn a_read_queue_wakes_once_a_read_is_ready_to_hand_over() -> io::Result<()> {
    use core::sync::atomic::Ordering::SeqCst;

    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 32 * 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let wake = Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
    let mut queue = fs.read_queue();
    assert!(queue.set_wake(Arc::clone(&wake) as Arc<dyn crate::fs::ReadWake>));
    for tag in 0..32usize {
        queue.submit(QueuedRead {
            tag,
            file: Arc::clone(&files[tag % 2]),
            offset: (tag * 64) as u64,
            buf: vec![0; 64],
        });
    }
    let mut handed = 0usize;
    while queue.outstanding() > 0 {
        let rung = wake.0.load(SeqCst);
        queue.wait(0, &mut |done| {
            assert!(done.result.is_ok(), "read {} failed", done.tag);
            handed += 1;
        });
        assert!(
            handed >= rung,
            "{rung} wakes rung but {handed} reads handed over"
        );
        std::thread::yield_now();
    }
    assert_eq!(handed, 32);
    assert_eq!(
        wake.0.load(SeqCst),
        32,
        "one wake per read the ring finished"
    );
    Ok(())
}

/// A read of a file the ring cannot take is read serially, which blocks, so
/// a wait for none leaves it held, and a wait for one reads it.
#[test]
fn a_read_without_a_descriptor_waits_for_a_wait_for_one() -> io::Result<()> {
    use crate::fs::MemFs;

    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let mem = MemFs::new();
    let path = Path::new("/m");
    mem.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(b"hello, world")?;
    let file: Arc<dyn FsFile> = Arc::from(mem.open(path, &FsOpenOptions::new().read(true))?);
    assert!(
        file.backing_fd().is_none(),
        "an in-memory file has no descriptor"
    );

    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 1,
        file,
        offset: 7,
        buf: vec![0; 5],
    });
    let mut got = Vec::new();
    queue.wait(0, &mut |done| got.push(done.buf));
    assert!(got.is_empty(), "a wait for none reads nothing");
    assert_eq!((queue.outstanding(), queue.held()), (1, 1));
    queue.wait(1, &mut |done| got.push(done.buf));
    assert_eq!(got, [b"world".to_vec()]);
    assert_eq!((queue.outstanding(), queue.held()), (0, 0));
    Ok(())
}

/// A wait for none never blocks on the ring's submission channel: with the
/// ring thread held on a read that has no data yet and the channel full, it
/// keeps its reads and returns, and a later wait for one sends and reads them.
#[test]
fn a_wait_for_none_does_not_wait_for_room_on_the_ring() -> io::Result<()> {
    // Ring capacity, which bounds the submission channel.
    const ENTRIES: u32 = 2;
    let Some(_) = try_io_uring() else {
        return Ok(());
    };
    let fs = IoUringFs::with_ring_size(ENTRIES)?;
    let dir = tempfile::tempdir()?;
    let fifo = dir.path().join("fifo");
    let made = std::process::Command::new("mkfifo").arg(&fifo).status()?;
    assert!(made.success(), "mkfifo");
    // Read and write, so the open does not wait for a writer.
    let pipe: Arc<dyn FsFile> =
        Arc::from(fs.open(&fifo, &FsOpenOptions::new().read(true).write(true))?);
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let read_of = |file: &Arc<dyn FsFile>, len: usize| QueuedRead {
        tag: 0,
        file: Arc::clone(file),
        offset: 0,
        buf: vec![0; len],
    };

    // The ring thread takes this read and waits on it.
    let mut held = fs.read_queue();
    held.submit(read_of(&pipe, 1));
    held.wait(0, &mut |_| {});
    std::thread::sleep(std::time::Duration::from_millis(200));
    // Nothing drains the channel now: these fill it.
    let mut filling: Vec<_> = (0..ENTRIES)
        .map(|_| {
            let mut queue = fs.read_queue();
            queue.submit(read_of(&files[0], 64));
            queue.wait(0, &mut |_| {});
            queue
        })
        .collect();

    // The queue itself, which a thread can take, rather than the trait
    // object `read_queue` hands out.
    let mut late = UringReadQueue::new(&fs.inner);
    late.submit(read_of(&files[1], 64));
    std::thread::scope(|scope| {
        let (returned, polled) = std::sync::mpsc::channel();
        let poll = scope.spawn(move || {
            late.wait(0, &mut |_| {});
            returned.send(()).ok();
            late
        });
        let prompt = polled
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_ok();
        // Release the ring thread either way, so the test ends.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fifo)
            .and_then(|mut writer| writer.write_all(b"x"))
            .expect("write to the fifo");
        let mut late = poll.join().expect("the poll thread");
        assert!(prompt, "a wait for none waited for room on the ring");

        assert_eq!(late.outstanding(), 1, "the read is kept, not lost");
        assert_eq!(
            late.held(),
            1,
            "a read no ring has is one only a wait carries out"
        );
        let mut got = Vec::new();
        while late.outstanding() > 0 {
            late.wait(1, &mut |done| got.push(done.result.is_ok()));
        }
        assert_eq!(got, [true], "a later wait sends and reads it");
    });
    for queue in &mut filling {
        while queue.outstanding() > 0 {
            queue.wait(1, &mut |done| assert!(done.result.is_ok()));
        }
    }
    while held.outstanding() > 0 {
        held.wait(1, &mut |done| assert!(done.result.is_ok()));
    }
    Ok(())
}

/// A wait for none does not wait for the submission lock either: while
/// another sender holds it (blocked on a full channel, say), the wait keeps
/// its reads, reports them held, and a later wait sends them.
#[test]
fn a_wait_for_none_does_not_wait_for_the_submission_lock() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = UringReadQueue::new(&fs.inner);
    queue.submit(QueuedRead {
        tag: 0,
        file: Arc::clone(&files[0]),
        offset: 0,
        buf: vec![0; 64],
    });

    let lock = fs
        .inner
        .tx
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut queue = std::thread::scope(|scope| {
        let (returned, polled) = std::sync::mpsc::channel();
        let poll = scope.spawn(move || {
            queue.wait(0, &mut |_| {});
            returned.send(()).ok();
            queue
        });
        let prompt = polled
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_ok();
        drop(lock);
        let queue = poll.join().expect("the poll thread");
        assert!(prompt, "a wait for none waited for the submission lock");
        queue
    });
    assert_eq!((queue.outstanding(), queue.held()), (1, 1));
    let mut got = Vec::new();
    while queue.outstanding() > 0 {
        queue.wait(1, &mut |done| got.push(done.result.is_ok()));
    }
    assert_eq!(got, [true], "a later wait sends and reads it");
    Ok(())
}

/// A wake asked for once reads are on the ring is declined: those reads
/// report to a sink without it, and a caller sleeping on it would not wake.
#[test]
fn a_wake_asked_for_with_reads_in_flight_is_declined() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let wake = Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
    let mut queue = UringReadQueue::new(&fs.inner);
    queue.submit(QueuedRead {
        tag: 0,
        file: Arc::clone(&files[0]),
        offset: 0,
        buf: vec![0; 64],
    });
    queue.issue(true);
    assert!(!queue.set_wake(Arc::clone(&wake) as Arc<dyn crate::fs::ReadWake>));
    while queue.outstanding() > 0 {
        queue.wait(1, &mut |done| assert!(done.result.is_ok()));
    }
    assert!(
        queue.set_wake(wake as Arc<dyn crate::fs::ReadWake>),
        "nothing in flight"
    );
    Ok(())
}

/// A read past the end of its file comes back failed as a short read, and
/// the reads beside it still come back read.
#[test]
fn a_read_queue_reports_a_short_read_as_failed() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = fs.read_queue();
    for (tag, offset) in [(0usize, 0u64), (1, 1 << 20), (2, 64)] {
        queue.submit(QueuedRead {
            tag,
            file: Arc::clone(&files[0]),
            offset,
            buf: vec![0; 64],
        });
    }
    let mut got = Vec::new();
    while queue.outstanding() > 0 {
        queue.wait(1, &mut |done| {
            got.push((done.tag, done.result.map_err(|e| e.kind())));
        });
    }
    got.sort_unstable_by_key(|(tag, _)| *tag);
    assert_eq!(
        got,
        [
            (0, Ok(())),
            (1, Err(crate::io::ErrorKind::UnexpectedEof)),
            (2, Ok(()))
        ]
    );
    Ok(())
}

/// A queue reused for many rounds keeps no slot for a read it handed back:
/// its bookkeeping follows the reads in flight, not every read it has sent.
#[test]
fn a_reused_read_queue_keeps_no_slot_for_a_read_handed_back() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 8 * 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = UringReadQueue::new(&fs.inner);
    for round in 0..100usize {
        for i in 0..8usize {
            queue.submit(QueuedRead {
                tag: round * 8 + i,
                file: Arc::clone(&files[i % 2]),
                offset: (i * 64) as u64,
                buf: vec![0; 64],
            });
        }
        while queue.outstanding() > 0 {
            queue.wait(1, &mut |done| {
                assert!(done.result.is_ok(), "read {} failed", done.tag);
            });
        }
        assert!(
            queue.sent.is_empty(),
            "round {round} kept {} slots",
            queue.sent.len()
        );
    }
    Ok(())
}

/// The results of draining `queue` to empty, by tag, with each error's kind.
fn drain_results(queue: &mut dyn ReadQueue) -> Vec<(usize, Result<(), crate::io::ErrorKind>)> {
    let mut got = Vec::new();
    while queue.outstanding() > 0 {
        queue.wait(1, &mut |done| {
            got.push((done.tag, done.result.map_err(|e| e.kind())));
        });
    }
    got.sort_unstable_by_key(|(tag, _)| *tag);
    got
}

/// A read the ring need not take is handed back without it: an empty one at
/// once, and one too long for a submission queue entry as failed, beside a
/// read the ring serves.
#[test]
fn a_read_queue_hands_back_reads_the_ring_cannot_take() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 0,
        file: Arc::clone(&files[0]),
        offset: 0,
        buf: Vec::new(),
    });
    // Zeroed and never touched, so it costs address space, not memory.
    let too_long = usize::try_from(i32::MAX).expect("fits") + 1;
    queue.submit(QueuedRead {
        tag: 1,
        file: Arc::clone(&files[0]),
        offset: 0,
        buf: vec![0; too_long],
    });
    queue.submit(QueuedRead {
        tag: 2,
        file: Arc::clone(&files[0]),
        offset: 64,
        buf: vec![0; 64],
    });
    assert_eq!(
        drain_results(&mut *queue),
        [
            (0, Ok(())),
            (1, Err(crate::io::ErrorKind::InvalidInput)),
            (2, Ok(())),
        ]
    );
    Ok(())
}

/// A file with no descriptor is read serially, and what that read returns is
/// handed back as is: short of its block as failed, a refused read with its
/// error.
#[test]
fn a_serial_read_reports_a_short_read_and_a_refused_one() -> io::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};

    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let mem = MemFs::new();
    let path = Path::new("/m");
    mem.open(path, &FsOpenOptions::new().write(true).create(true))?
        .write_all(b"hello, world")?;
    let short: Arc<dyn FsFile> = Arc::from(mem.open(path, &FsOpenOptions::new().read(true))?);
    let faulty = FaultFs::new(mem.clone());
    faulty.injector().arm(FaultRule::new(
        FaultOp::ReadAt,
        Fault::Error(crate::io::ErrorKind::PermissionDenied),
    ));
    let refused: Arc<dyn FsFile> = Arc::from(faulty.open(path, &FsOpenOptions::new().read(true))?);

    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 0,
        file: short,
        offset: 7,
        buf: vec![0; 64],
    });
    queue.submit(QueuedRead {
        tag: 1,
        file: refused,
        offset: 0,
        buf: vec![0; 5],
    });
    assert_eq!(
        drain_results(&mut *queue),
        [
            (0, Err(crate::io::ErrorKind::UnexpectedEof)),
            (1, Err(crate::io::ErrorKind::PermissionDenied)),
        ]
    );
    Ok(())
}

/// Reads sent to a ring thread that has shut down fail, whether the wait
/// would block for room or not, and none is left outstanding.
#[test]
fn a_read_queue_fails_its_reads_once_the_ring_thread_is_gone() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
        .into_iter()
        .map(Arc::from)
        .collect();
    // What a shutdown leaves: no sender to the ring thread.
    drop(
        fs.inner
            .tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take(),
    );
    for min in [0usize, 1] {
        let mut queue = fs.read_queue();
        queue.submit(QueuedRead {
            tag: 0,
            file: Arc::clone(&files[0]),
            offset: 0,
            buf: vec![0; 64],
        });
        let mut got = Vec::new();
        queue.wait(min, &mut |done| {
            got.push((done.tag, done.result.map_err(|e| e.kind())));
        });
        assert_eq!(
            got,
            [(0, Err(crate::io::ErrorKind::BrokenPipe))],
            "a wait for {min}"
        );
        assert_eq!(queue.outstanding(), 0, "a wait for {min}");
    }
    Ok(())
}

/// A submission lock poisoned by a panicking sender still sends: the
/// channel it guards is intact, so a wait for none puts the reads on the ring.
#[test]
fn a_poisoned_submission_lock_still_sends() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
        .into_iter()
        .map(Arc::from)
        .collect();
    std::thread::scope(|scope| {
        let poisoner = scope.spawn(|| {
            let _held = fs
                .inner
                .tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            panic!("a sender panics holding the submission lock");
        });
        assert!(poisoner.join().is_err(), "the sender panicked");
    });
    assert!(fs.inner.tx.is_poisoned(), "the lock is poisoned");

    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 0,
        file: Arc::clone(&files[0]),
        offset: 64,
        buf: vec![0; 64],
    });
    let mut got = Vec::new();
    while queue.outstanding() > 0 {
        queue.wait(0, &mut |done| got.push((done.tag, done.result.is_ok())));
        std::thread::yield_now();
    }
    assert_eq!(got, [(0, true)]);
    Ok(())
}

/// The ring's result for a read is handed back as the read's verdict: a
/// negative result is the read's `errno`. Completions may arrive in any order:
/// the slots of handed-back reads are dropped from either end, and a
/// completion for a read already handed back hands back nothing.
#[test]
fn a_read_queue_hands_back_completions_in_any_order() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
        .into_iter()
        .map(Arc::from)
        .collect();
    let mut queue = UringReadQueue::new(&fs.inner);
    // Two reads as if on the ring, at positions 0 and 1.
    for tag in 0..2usize {
        queue.sent.push_back(Some(QueuedRead {
            tag,
            file: Arc::clone(&files[0]),
            offset: 0,
            buf: vec![0; 64],
        }));
    }
    queue.on_ring = 2;

    /// `EACCES` on Linux.
    const EACCES: i32 = 13;
    // Every completion is taken before any assertion, so a failing one leaves
    // nothing for the queue's drop to wait for.
    let last = queue.complete(1, -EACCES);
    let slots_after_last = queue.sent.len();
    let again = queue.complete(1, 64);
    let first = queue.complete(0, 64);

    let last = last.expect("the read at 1");
    assert_eq!(
        last.result.map_err(|e| e.kind()),
        Err(crate::io::ErrorKind::PermissionDenied),
        "a negative result is the read's errno"
    );
    assert_eq!(
        slots_after_last, 1,
        "the handed-back slot at the end is dropped"
    );
    assert!(
        again.is_none(),
        "a completion for a read handed back hands back nothing"
    );
    assert!(first.expect("the read at 0").result.is_ok());
    assert!(queue.sent.is_empty(), "no slot outlives its read");
    assert_eq!(queue.on_ring, 0);
    Ok(())
}

/// Dropping a queue with reads in flight waits their completions out
/// before their buffers go, and the backend stays usable after.
#[test]
fn dropping_a_read_queue_with_reads_in_flight_leaves_the_backend_usable() -> io::Result<()> {
    let Some(fs) = try_io_uring() else {
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 128 * 64)?
        .into_iter()
        .map(Arc::from)
        .collect();
    for _ in 0..50 {
        let mut queue = fs.read_queue();
        for tag in 0..128usize {
            queue.submit(QueuedRead {
                tag,
                file: Arc::clone(&files[tag % 2]),
                offset: (tag * 64) as u64,
                buf: vec![0; 64],
            });
        }
        // Puts every read on the ring without waiting for any.
        queue.wait(0, &mut |_| {});
        drop(queue);
    }
    let mut buf = [0u8; 64];
    let n = files[1].read_at(&mut buf, 64)?;
    assert_eq!(n, 64);
    assert_eq!(buf.to_vec(), expected_bytes(1, 64, 64));
    Ok(())
}
