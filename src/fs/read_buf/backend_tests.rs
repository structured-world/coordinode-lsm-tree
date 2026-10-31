use crate::fs::{
    CountingFs, CrashFs, FaultFs, Fs, FsFile, FsMetadata, FsOpenOptions, MemFs, ReadBuf, StdFs,
};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A file `bytes` long on `fs`, open for reading.
fn file_of(fs: &dyn Fs, path: &Path, bytes: &[u8]) -> crate::Result<Box<dyn FsFile>> {
    let mut file = fs.open(path, &FsOpenOptions::new().write(true).create(true))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    Ok(fs.open(path, &FsOpenOptions::new().read(true))?)
}

/// A file written outside this crate, as safe code may be written: it overrides
/// nothing, so every read goes through the trait's defaults.
struct Foreign {
    inner: Box<dyn FsFile>,
    /// How many bytes `read_at` reports past what it read.
    overclaim: usize,
    /// Nonzero bytes `read_at` found in the buffers it was handed.
    nonzero_seen: AtomicUsize,
}

impl Foreign {
    fn over(bytes: &[u8]) -> crate::Result<Self> {
        Ok(Self {
            inner: file_of(&MemFs::new(), Path::new("/f"), bytes)?,
            overclaim: 0,
            nonzero_seen: AtomicUsize::new(0),
        })
    }
}

/// A file whose reads report success having filled nothing.
struct Lying(Foreign);

macro_rules! file_over {
    ($file:ty, $inner:expr) => {
        impl Read for $file {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let inner: fn(&mut Self) -> &mut Box<dyn FsFile> = $inner;
                inner(self).read(buf)
            }
        }

        impl Write for $file {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let inner: fn(&mut Self) -> &mut Box<dyn FsFile> = $inner;
                inner(self).write(buf)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                let inner: fn(&mut Self) -> &mut Box<dyn FsFile> = $inner;
                inner(self).flush()
            }
        }

        impl Seek for $file {
            fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
                let inner: fn(&mut Self) -> &mut Box<dyn FsFile> = $inner;
                inner(self).seek(to)
            }
        }
    };
}

file_over!(Foreign, |file| &mut file.inner);
file_over!(Lying, |file| &mut file.0.inner);

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
        let read = self.inner.read_at(buf, offset)?;
        Ok(read + self.overclaim)
    }
    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.inner.lock_exclusive()
    }
}

impl FsFile for Lying {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.0.sync_all()
    }
    fn sync_data(&self) -> crate::io::Result<()> {
        self.0.sync_data()
    }
    fn metadata(&self) -> crate::io::Result<FsMetadata> {
        self.0.metadata()
    }
    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.0.set_len(size)
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        self.0.read_at(buf, offset)
    }
    fn read_at_buf(&self, _buf: &mut ReadBuf<'_>, _offset: u64) -> crate::io::Result<()> {
        Ok(())
    }
    fn read_many_buf(&self, _regions: &mut [(u64, ReadBuf<'_>)]) -> crate::io::Result<()> {
        Ok(())
    }
    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.0.lock_exclusive()
    }
}

/// A backend written outside the crate is never shown memory nobody wrote,
/// whatever freed allocation the buffer reuses.
#[test]
fn a_foreign_read_at_sees_only_zeroes() -> crate::Result<()> {
    const LEN: usize = 64 * 1024;
    let content = vec![7u8; LEN];
    let file = Foreign::over(&content)?;
    for _ in 0..8 {
        drop(vec![0xAAu8; LEN]);
        let bytes = crate::file::read_exact(&file, 0, LEN)?;
        assert_eq!(&*bytes, &content[..]);
    }
    assert_eq!(file.nonzero_seen.load(Ordering::Relaxed), 0);
    Ok(())
}

/// As above through the batch, which the default answers with `read_many`.
#[test]
fn a_foreign_read_many_sees_only_zeroes() -> crate::Result<()> {
    const LEN: usize = 64 * 1024;
    let content = vec![7u8; 2 * LEN];
    let file = Foreign::over(&content)?;
    for _ in 0..8 {
        drop(vec![0xAAu8; LEN]);
        let bytes = crate::file::read_exact_many(&file, &[(0, LEN), (LEN as u64, LEN)])?;
        assert_eq!(bytes.len(), 2);
        assert!(bytes.iter().all(|bytes| **bytes == content[..LEN]));
    }
    assert_eq!(file.nonzero_seen.load(Ordering::Relaxed), 0);
    Ok(())
}

/// The default's batch fails a region the file cannot fill.
#[test]
fn a_foreign_read_many_refuses_a_short_region() -> crate::Result<()> {
    let file = Foreign::over(&[7; 64])?;
    let error = crate::file::read_exact_many(&file, &[(32, 64)]).expect_err("past the end");
    assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
    Ok(())
}

/// An implementation that reports success having filled nothing gets an error,
/// not a slice over memory nobody wrote.
#[test]
fn read_exact_refuses_a_read_that_filled_nothing() -> crate::Result<()> {
    let file = Lying(Foreign::over(&[7; 128])?);
    let error = crate::file::read_exact(&file, 0, 128).expect_err("nothing was filled");
    assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
    Ok(())
}

/// As above for a batch.
#[test]
fn read_exact_many_refuses_a_read_that_filled_nothing() -> crate::Result<()> {
    let file = Lying(Foreign::over(&[7; 128])?);
    let error =
        crate::file::read_exact_many(&file, &[(0, 64), (64, 64)]).expect_err("nothing was filled");
    assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof);
    Ok(())
}

/// A backend reporting more bytes than its buffer holds breaks the contract of
/// `read_at`; that is an I/O error, not a panic on a read path.
#[test]
fn read_exact_refuses_a_read_past_its_buffer() -> crate::Result<()> {
    let mut file = Foreign::over(&[7; 128])?;
    file.overclaim = 1;
    let error = crate::file::read_exact(&file, 0, 128).expect_err("one byte too many");
    assert_eq!(error.kind(), crate::io::ErrorKind::InvalidData);
    Ok(())
}

/// Every backend and wrapper of the crate reads into memory nobody wrote: a
/// read and a batch bring back the file's bytes, and a region past the end of
/// the file fails.
#[test]
fn every_backend_fills_an_uninitialized_buffer() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let content: Vec<u8> = (0..=255).collect();
    let fs: [(Arc<dyn Fs>, &str); 5] = [
        (Arc::new(MemFs::new()), "/mem"),
        (Arc::new(StdFs), "std"),
        (Arc::new(CountingFs::new(Arc::new(MemFs::new()))), "/count"),
        (Arc::new(CrashFs::new(MemFs::new())), "/crash"),
        (Arc::new(FaultFs::new(MemFs::new())), "/fault"),
    ];
    for (fs, name) in fs {
        let path = if name.starts_with('/') {
            Path::new(name).to_path_buf()
        } else {
            dir.path().join(name)
        };
        let file = file_of(&*fs, &path, &content)?;
        assert_eq!(
            &*crate::file::read_exact(&*file, 16, 32)?,
            &content[16..48],
            "{name}"
        );
        let many = crate::file::read_exact_many(&*file, &[(8, 16), (128, 32)])?;
        assert_eq!(&*many[0], &content[8..24], "{name}");
        assert_eq!(&*many[1], &content[128..160], "{name}");
        let error = crate::file::read_exact_many(&*file, &[(240, 32)]).expect_err("past the end");
        assert_eq!(error.kind(), crate::io::ErrorKind::UnexpectedEof, "{name}");
    }
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
