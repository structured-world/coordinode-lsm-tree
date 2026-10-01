#![expect(clippy::expect_used, reason = "test code")]

use super::{
    BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, MemFs, QueuedRead, ReadDone,
};
use crate::io;
use crate::path::Path;
use alloc::sync::Arc;
use test_log::test;

/// How a [`Scripted`] backend answers a batched read.
#[derive(Clone, Copy)]
enum Answer {
    /// Reads every request.
    Serve,
    /// Reads and hands over the first `n` requests, then fails.
    FailAfter(usize),
    /// Reports success without reading anything.
    ClaimWithoutFilling,
}

/// A backend over a [`MemFs`] whose batched read answers as scripted; the
/// rest delegates.
struct Scripted {
    inner: MemFs,
    answer: Answer,
}

impl Fs for Scripted {
    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> io::Result<()> {
        match self.answer {
            Answer::Serve => self.inner.read_blocks_batched_each(reqs, on_read),
            Answer::FailAfter(n) => {
                let (served, _) = reqs.split_at_mut(n);
                self.inner.read_blocks_batched_each(served, on_read)?;
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refused by the scripted backend",
                ))
            }
            Answer::ClaimWithoutFilling => Ok(()),
        }
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<alloc::boxed::Box<dyn FsFile>> {
        self.inner.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<alloc::vec::Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }
}

/// A backend answering as `answer`, and a file on it holding `0, 1, …, 99`.
fn backend(answer: Answer) -> (Scripted, Arc<dyn FsFile>) {
    let inner = MemFs::new();
    inner.create_dir_all(Path::new("/d")).expect("dir");
    let path = Path::new("/d/f");
    let bytes: alloc::vec::Vec<u8> = (0u8..100).collect();
    inner
        .open(path, &FsOpenOptions::new().write(true).create(true))
        .expect("create")
        .write_all(&bytes)
        .expect("write");
    let file = Arc::from(
        inner
            .open(path, &FsOpenOptions::new().read(true))
            .expect("open"),
    );
    (Scripted { inner, answer }, file)
}

/// Submits three reads of four bytes at offsets 0, 10 and 20, tagged 5, 6 and
/// 7, waits, and returns what came back.
fn three_reads(fs: &Scripted, file: &Arc<dyn FsFile>) -> alloc::vec::Vec<ReadDone> {
    let mut queue = fs.read_queue();
    for (tag, offset) in [(5, 0), (6, 10), (7, 20)] {
        queue.submit(QueuedRead {
            tag,
            file: Arc::clone(file),
            offset,
            buf: alloc::vec![0; 4],
        });
    }
    assert_eq!(queue.outstanding(), 3);
    let mut done = alloc::vec::Vec::new();
    queue.wait(1, &mut |read| done.push(read));
    assert_eq!(
        queue.outstanding(),
        0,
        "the default queue reads what it holds"
    );
    done
}

/// Every read comes back with its tag and its bytes.
#[test]
fn a_queue_hands_back_each_read_with_its_tag_and_bytes() {
    let (fs, file) = backend(Answer::Serve);
    let done = three_reads(&fs, &file);
    let got: alloc::vec::Vec<(usize, alloc::vec::Vec<u8>, bool)> = done
        .into_iter()
        .map(|read| (read.tag, read.buf, read.result.is_ok()))
        .collect();
    assert_eq!(
        got,
        [
            (5, alloc::vec![0, 1, 2, 3], true),
            (6, alloc::vec![10, 11, 12, 13], true),
            (7, alloc::vec![20, 21, 22, 23], true),
        ]
    );
}

/// A batch that fails after serving some of its reads hands those back read,
/// and the others failed with the batch's kind of error: none is lost, and
/// none that was not read is reported read.
#[test]
fn a_failed_batch_hands_back_every_read_it_did_not_serve_as_failed() {
    let (fs, file) = backend(Answer::FailAfter(1));
    let done = three_reads(&fs, &file);
    assert_eq!(done.len(), 3, "every read comes back");
    let first = done.first().expect("three reads");
    assert_eq!(first.tag, 5);
    assert!(first.result.is_ok(), "the served read is read");
    assert_eq!(first.buf, [0, 1, 2, 3]);
    for read in done.iter().skip(1) {
        let error = read.result.as_ref().expect_err("an unserved read failed");
        assert_eq!(
            error.kind(),
            io::ErrorKind::PermissionDenied,
            "tag {}: the batch's kind of error",
            read.tag
        );
    }
}

/// A backend that reports success without reading hands every read back
/// failed, not read: the bytes it would have been decoded from were never
/// written.
#[test]
fn a_read_reported_done_without_being_filled_comes_back_failed() {
    let (fs, file) = backend(Answer::ClaimWithoutFilling);
    let done = three_reads(&fs, &file);
    assert_eq!(done.len(), 3);
    for read in &done {
        let error = read.result.as_ref().expect_err("an unfilled read failed");
        assert_eq!(
            error.kind(),
            io::ErrorKind::UnexpectedEof,
            "tag {}",
            read.tag
        );
    }
}

/// A wait for none of the reads is a look at what is ready: the default
/// queue reads only when it is waited on, so it reads nothing and keeps what
/// it holds, which a wait for one then reads.
#[test]
fn a_wait_for_no_read_reads_nothing() {
    let (fs, file) = backend(Answer::Serve);
    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 5,
        file: Arc::clone(&file),
        offset: 0,
        buf: alloc::vec![0; 4],
    });
    let mut handed = 0;
    queue.wait(0, &mut |_| handed += 1);
    assert_eq!(handed, 0, "nothing was ready");
    assert_eq!(queue.outstanding(), 1, "the read is still held");
    queue.wait(1, &mut |_| handed += 1);
    assert_eq!(handed, 1);
    assert_eq!(queue.outstanding(), 0);
}

/// Waiting on a queue that holds nothing hands nothing over and does not ask
/// the backend for anything.
#[test]
fn waiting_on_an_empty_queue_hands_nothing_over() {
    let (fs, _file) = backend(Answer::ClaimWithoutFilling);
    let mut queue = fs.read_queue();
    let mut handed = 0;
    queue.wait(1, &mut |_| handed += 1);
    assert_eq!(handed, 0);
    assert_eq!(queue.outstanding(), 0);
}
