use crate::fs::{Fs, FsFile, FsMetadata, FsOpenOptions, MemFs, ReadBuf};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A file written outside this crate, as safe code may be written.
struct Foreign {
    inner: Box<dyn FsFile>,
    /// Whether `read_at_buf` claims success without filling anything.
    lies: bool,
    /// Nonzero bytes `read_at` found in the buffers it was handed.
    nonzero_seen: AtomicUsize,
}

impl Foreign {
    fn over(bytes: &[u8], lies: bool) -> crate::Result<Self> {
        let fs = MemFs::new();
        let path = Path::new("/f");
        let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
        file.write_all(bytes)?;
        drop(file);
        Ok(Self {
            inner: fs.open(path, &FsOpenOptions::new().read(true))?,
            lies,
            nonzero_seen: AtomicUsize::new(0),
        })
    }
}

impl Read for Foreign {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for Foreign {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for Foreign {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(to)
    }
}

impl FsFile for Foreign {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.inner.sync_all()
    }
    fn sync_data(&self) -> crate::io::Result<()> {
        self.inner.sync_data()
    }
    fn metadata(&self) -> crate::io::Result<FsMetadata> {
        self.inner.metadata()
    }
    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.inner.set_len(size)
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        // Safe code may look at what it was given: that must be initialized.
        let nonzero = buf.iter().filter(|byte| **byte != 0).count();
        self.nonzero_seen.fetch_add(nonzero, Ordering::Relaxed);
        self.inner.read_at(buf, offset)
    }
    fn read_at_buf(&self, buf: &mut ReadBuf<'_>, offset: u64) -> crate::io::Result<()> {
        if self.lies {
            return Ok(());
        }
        let filled = self.read_at(buf.init_mut(), offset)?;
        buf.set_filled(filled);
        Ok(())
    }
    fn read_many_buf(&self, regions: &mut [(u64, ReadBuf<'_>)]) -> crate::io::Result<()> {
        if self.lies {
            return Ok(());
        }
        crate::fs::read_buf::read_each(self, regions)
    }
    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.inner.lock_exclusive()
    }
}

/// A backend written outside the crate is never shown memory nobody wrote,
/// whatever freed allocation the buffer reuses.
#[test]
fn a_foreign_read_at_sees_only_zeroes() -> crate::Result<()> {
    const LEN: usize = 64 * 1024;
    let content = vec![7u8; LEN];
    let file = Foreign::over(&content, false)?;
    for _ in 0..8 {
        drop(vec![0xAAu8; LEN]);
        let bytes = crate::file::read_exact(&file, 0, LEN)?;
        assert_eq!(&*bytes, &content[..]);
    }
    assert_eq!(file.nonzero_seen.load(Ordering::Relaxed), 0);
    Ok(())
}

/// An implementation that reports success having filled nothing gets an error,
/// not a slice over memory nobody wrote.
#[test]
fn read_exact_refuses_a_read_that_filled_nothing() -> crate::Result<()> {
    let file = Foreign::over(&[7; 128], true)?;
    let error = crate::file::read_exact(&file, 0, 128).expect_err("nothing was filled");
    assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
    Ok(())
}

/// As above for a batch.
#[test]
fn read_exact_many_refuses_a_read_that_filled_nothing() -> crate::Result<()> {
    let file = Foreign::over(&[7; 128], true)?;
    let error =
        crate::file::read_exact_many(&file, &[(0, 64), (64, 64)]).expect_err("nothing was filled");
    assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
    Ok(())
}

/// A safe reader is never shown memory nobody wrote either.
#[test]
fn from_reader_shows_a_safe_reader_only_zeroes() -> std::io::Result<()> {
    struct Looking {
        nonzero_seen: usize,
    }
    impl Read for Looking {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.nonzero_seen += buf.iter().filter(|byte| **byte != 0).count();
            buf.fill(1);
            Ok(buf.len())
        }
    }
    const LEN: usize = 64 * 1024;
    let content = vec![1u8; LEN];
    let mut reader = Looking { nonzero_seen: 0 };
    for _ in 0..8 {
        drop(vec![0xAAu8; LEN]);
        let bytes = crate::Slice::from_reader(&mut reader, LEN)?;
        assert_eq!(&*bytes, &content[..]);
    }
    assert_eq!(reader.nonzero_seen, 0);
    Ok(())
}
