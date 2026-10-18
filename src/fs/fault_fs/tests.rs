use super::*;
use crate::fs::{MemFs, StdFs};
use crate::io::ErrorKind;
use std::io::{Read, Seek, SeekFrom, Write};
use test_log::test;

/// Opens `/d/<name>` for writing on `fs`, creating the parent dir first.
fn write_file(fs: &dyn Fs, name: &str) -> io::Result<Box<dyn FsFile>> {
    fs.create_dir_all(Path::new("/d"))?;
    let path = format!("/d/{name}");
    fs.open(
        Path::new(&path),
        &FsOpenOptions::new().write(true).create(true),
    )
}

#[test]
fn no_rules_pass_through() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    let mut file = write_file(&fs, "f")?;
    file.write_all(b"hello world")?;
    drop(file);

    let mut buf = String::new();
    fs.open(Path::new("/d/f"), &FsOpenOptions::new().read(true))?
        .read_to_string(&mut buf)?;
    assert_eq!(buf, "hello world");
    Ok(())
}

#[test]
fn error_on_write_surfaces_with_kind() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.injector().arm(FaultRule::new(
        FaultOp::Write,
        Fault::Error(ErrorKind::PermissionDenied),
    ));

    let mut file = write_file(&fs, "f")?;
    let err = file
        .write_all(b"data")
        .expect_err("armed Write fault must surface");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    Ok(())
}

#[test]
fn short_write_truncates_the_buffer() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    // Every write accepts at most 3 bytes.
    fs.injector()
        .arm(FaultRule::new(FaultOp::Write, Fault::ShortWrite(3)));

    let mut file = write_file(&fs, "f")?;
    let n = file.write(b"hello").expect("short write returns Ok(n)");
    assert_eq!(n, 3, "only 3 of 5 bytes accepted");
    drop(file);

    let mut buf = String::new();
    fs.open(Path::new("/d/f"), &FsOpenOptions::new().read(true))?
        .read_to_string(&mut buf)?;
    assert_eq!(buf, "hel", "only the accepted prefix is persisted");
    Ok(())
}

#[test]
fn short_write_zero_is_a_stuck_writer() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.injector()
        .arm(FaultRule::new(FaultOp::Write, Fault::ShortWrite(0)));

    let mut file = write_file(&fs, "f")?;
    // write_all over a writer that accepts 0 bytes is WriteZero.
    let err = file
        .write_all(b"data")
        .expect_err("0-byte short write stalls write_all");
    assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    Ok(())
}

#[test]
fn path_filter_scopes_the_fault() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.injector()
        .arm(FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path("target"));

    // Write to the targeted file fails.
    let mut targeted = write_file(&fs, "target")?;
    assert!(
        targeted.write_all(b"x").is_err(),
        "write to /d/target is faulted"
    );

    // Write to a different file is untouched.
    let mut other = write_file(&fs, "other")?;
    other
        .write_all(b"x")
        .expect("write to /d/other is not faulted");
    Ok(())
}

#[test]
fn skip_lets_initial_matches_pass_then_fires() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    // Skip the first two opens; fail from the third onward.
    fs.injector()
        .arm(FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).skip(2));
    fs.create_dir_all(Path::new("/d"))?;
    let opts = FsOpenOptions::new().write(true).create(true);

    fs.open(Path::new("/d/a"), &opts).expect("1st open passes");
    fs.open(Path::new("/d/b"), &opts).expect("2nd open passes");
    assert!(
        fs.open(Path::new("/d/c"), &opts).is_err(),
        "3rd open is faulted"
    );
    Ok(())
}

#[test]
fn once_is_exhausted_after_the_first_fire() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.injector()
        .arm(FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other)).once());

    let file = write_file(&fs, "f")?;
    assert!(file.sync_all().is_err(), "first sync_all is faulted");
    file.sync_all()
        .expect("second sync_all is no longer faulted");
    Ok(())
}

#[test]
fn clear_removes_armed_rules() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    let inj = fs.injector();
    inj.arm(FaultRule::new(
        FaultOp::Write,
        Fault::Error(ErrorKind::Other),
    ));
    inj.clear();

    let mut file = write_file(&fs, "f")?;
    file.write_all(b"data")
        .expect("cleared rule no longer faults");
    Ok(())
}

#[test]
fn rename_fault_matches_the_destination() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    fs.injector()
        .arm(FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other)).on_path("manifest"));
    fs.create_dir_all(Path::new("/d"))?;
    let opts = FsOpenOptions::new().write(true).create(true);

    // Two source files to rename.
    drop(fs.open(Path::new("/d/tmp1"), &opts)?);
    drop(fs.open(Path::new("/d/tmp2"), &opts)?);

    assert!(
        fs.rename(Path::new("/d/tmp1"), Path::new("/d/manifest"))
            .is_err(),
        "rename whose destination contains 'manifest' is faulted"
    );
    fs.rename(Path::new("/d/tmp2"), Path::new("/d/other"))
        .expect("rename to a non-matching destination is not faulted");
    Ok(())
}

#[test]
fn read_at_fault_surfaces() -> io::Result<()> {
    let fs = FaultFs::new(MemFs::new());
    let mut file = write_file(&fs, "f")?;
    file.write_all(b"payload")?;
    drop(file);

    fs.injector().arm(FaultRule::new(
        FaultOp::ReadAt,
        Fault::Error(ErrorKind::Other),
    ));

    let file = fs.open(Path::new("/d/f"), &FsOpenOptions::new().read(true))?;
    let mut buf = [0u8; 4];
    assert!(
        file.read_at(&mut buf, 0).is_err(),
        "armed ReadAt fault surfaces on positional read"
    );
    Ok(())
}

#[test]
fn delegates_the_full_surface_when_unarmed() -> io::Result<()> {
    // With no rules armed, FaultFs must be a faithful pass-through to its inner
    // backend across the whole Fs / FsFile surface. Exercises every delegating
    // method so a regression in any forward is caught.
    let fs = FaultFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d"))?;
    fs.create_dir(Path::new("/d/sub"))?;

    // Open + the full FsFile surface, no fault armed.
    let mut f = fs.open(
        Path::new("/d/f"),
        &FsOpenOptions::new().read(true).write(true).create(true),
    )?;
    f.write_all(b"hello world")?;
    f.flush()?;
    f.sync_data()?;
    f.sync_all()?;
    f.sync_data_with(SyncMode::Normal)?;
    f.sync_all_with(SyncMode::Full)?;
    assert_eq!(f.seek(SeekFrom::Start(0))?, 0);
    let mut buf = [0u8; 5];
    assert_eq!(f.read(&mut buf)?, 5);
    assert_eq!(&buf, b"hello");
    assert_eq!(f.read_at(&mut buf, 6)?, 5);
    assert_eq!(&buf, b"world");
    assert_eq!(FsFile::metadata(&*f)?.len, 11);
    f.set_len(11)?;
    f.hint(FileHint::Random)?;
    assert!(f.try_lock_exclusive()?);
    f.lock_exclusive()?;
    drop(f);

    // Fs-level metadata / existence / listing.
    assert_eq!(fs.metadata(Path::new("/d/f"))?.len, 11);
    assert!(fs.exists(Path::new("/d/f"))?);
    assert!(!fs.exists(Path::new("/d/missing"))?);
    assert!(!fs.read_dir(Path::new("/d"))?.is_empty());

    // Directory + whole-file sync, identity / capability probes.
    fs.sync_directory(Path::new("/d"))?;
    fs.sync_directory_with(Path::new("/d"), SyncMode::Full)?;
    assert!(fs.backend_id().is_some());
    let _ = fs.volume_id(Path::new("/d"));
    assert!(fs.capabilities(Path::new("/d")).punch_hole);
    assert_eq!(fs.available_space(Path::new("/d"))?, u64::MAX);

    // Copy-style operations + best-effort hooks.
    fs.hard_link(Path::new("/d/f"), Path::new("/d/link"))?;
    fs.reflink_file(Path::new("/d/f"), Path::new("/d/clone"))?;
    fs.try_disable_cow(Path::new("/d/f"))?;
    fs.punch_hole(Path::new("/d/f"), 0, 4)?;
    let _ = fs.hard_link_count(Path::new("/d/f"));

    // Truncate reclaim + removal.
    fs.truncate_file(Path::new("/d/clone"))?;
    assert_eq!(fs.metadata(Path::new("/d/clone"))?.len, 0);
    fs.remove_file(Path::new("/d/link"))?;
    fs.remove_dir_all(Path::new("/d/sub"))?;
    Ok(())
}

#[test]
fn every_hookable_op_faults_when_armed() -> io::Result<()> {
    // Each FaultOp must actually gate its operation. Constructed via
    // `with_injector` (the shared-injector constructor) so that path is covered
    // too.
    let inj = Arc::new(FaultInjector::new());
    let fs = FaultFs::with_injector(MemFs::new(), Arc::clone(&inj));
    fs.create_dir_all(Path::new("/d"))?;
    {
        let mut f = fs.open(
            Path::new("/d/f"),
            &FsOpenOptions::new().write(true).create(true),
        )?;
        f.write_all(b"seed")?;
        f.sync_all()?;
    }

    for op in [
        FaultOp::CreateDirAll,
        FaultOp::CreateDir,
        FaultOp::RemoveFile,
        FaultOp::SyncDirectory,
        FaultOp::Read,
        FaultOp::Flush,
        FaultOp::SyncData,
        FaultOp::SyncAll,
        FaultOp::SetLen,
    ] {
        inj.arm(FaultRule::new(op, Fault::Error(ErrorKind::Other)));
    }

    // Fs-level ops (open is NOT armed, so handles still open).
    assert!(fs.create_dir_all(Path::new("/x")).is_err());
    assert!(fs.create_dir(Path::new("/d/sub")).is_err());
    assert!(fs.sync_directory(Path::new("/d")).is_err());
    assert!(
        fs.sync_directory_with(Path::new("/d"), SyncMode::Full)
            .is_err()
    );

    // File-handle ops.
    let mut f = fs.open(
        Path::new("/d/f"),
        &FsOpenOptions::new().read(true).write(true),
    )?;
    let mut buf = [0u8; 4];
    assert!(f.read(&mut buf).is_err(), "Read op faults");
    assert!(f.write(b"x").is_ok(), "Write op is not armed");
    assert!(f.flush().is_err(), "Flush op faults");
    assert!(f.sync_data().is_err(), "SyncData op faults");
    assert!(
        f.sync_data_with(SyncMode::Normal).is_err(),
        "sync_data_with routes through the SyncData op"
    );
    assert!(f.sync_all().is_err(), "SyncAll op faults");
    assert!(
        f.sync_all_with(SyncMode::Full).is_err(),
        "sync_all_with routes through the SyncAll op"
    );
    assert!(f.set_len(0).is_err(), "SetLen op faults");
    drop(f);

    assert!(
        fs.remove_file(Path::new("/d/f")).is_err(),
        "RemoveFile op faults"
    );
    Ok(())
}

#[test]
fn hard_link_count_path_accessor_faults_when_armed() {
    // The path-based `Fs::hard_link_count` accessor (used by the blob-file and
    // table reclaim probes) must consult the injector, not delegate blind, so an
    // armed `FaultOp::HardLinkCount` exercises its fail-closed path. MemFs
    // answers `1` for a file it holds (its `hard_link` copies, so bytes are
    // never shared); the injected fault must override that with `Other`, which
    // a blind delegation could not do.
    let inner = MemFs::new();
    inner.create_dir_all(Path::new("/d")).expect("create dir");
    drop(
        inner
            .open(
                Path::new("/d/f"),
                &crate::fs::FsOpenOptions::new().write(true).create(true),
            )
            .expect("create file"),
    );
    let fs = FaultFs::new(inner);
    let inj = fs.injector();

    let unarmed = fs
        .hard_link_count(Path::new("/d/f"))
        .expect("MemFs answers its own path accessor");
    assert_eq!(unarmed, 1, "MemFs never shares bytes between names");

    inj.arm(FaultRule::new(
        FaultOp::HardLinkCount,
        Fault::Error(ErrorKind::Other),
    ));
    let armed = fs
        .hard_link_count(Path::new("/d/f"))
        .expect_err("an armed FaultOp::HardLinkCount must gate the path accessor");
    assert_eq!(
        armed.kind(),
        ErrorKind::Other,
        "the injected fault must gate the path-based accessor, not delegate blind",
    );
}

#[test]
fn path_filtered_rule_never_matches_a_pathless_op() {
    // A rule with a path filter matches only operations that carry a path; a
    // path-less op (none of the current hook sites produce one, but the match
    // arm must stay correct) never matches.
    let rule = FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path("x");
    assert!(!rule.matches(FaultOp::Write, None));
    assert!(rule.matches(FaultOp::Write, Some(Path::new("/d/x"))));
    // No path filter matches a path-less op.
    let any = FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other));
    assert!(any.matches(FaultOp::Write, None));
}

#[test]
fn an_armed_reflink_rule_refuses_the_clone_and_an_unarmed_one_delegates() {
    // A checkpoint prefers a reflink clone where the filesystem offers one, so
    // a test that wants to fail the placement of a file has to be able to fail
    // that path too — otherwise the injection silently does nothing on a
    // copy-on-write filesystem and the test passes for the wrong reason.
    // Over a REAL backend with a real source file, so the unarmed case is
    // decided by the filesystem's own answer rather than by an error a
    // non-existent path would have produced anyway.
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src");
    std::fs::write(&src, b"payload").expect("write source");

    // Unarmed: the wrapper's verdict must equal the bare backend's on an
    // equivalent call, whatever that verdict is on this host.
    let bare = StdFs.reflink_file(&src, &dir.path().join("bare-dst"));
    let fs = FaultFs::new(StdFs);
    let wrapped = fs.reflink_file(&src, &dir.path().join("wrapped-dst"));
    assert_eq!(
        bare.is_ok(),
        wrapped.is_ok(),
        "an unarmed wrapper must pass the call through, not decide it: bare \
         {bare:?} vs wrapped {wrapped:?}",
    );

    fs.injector().arm(FaultRule::new(
        FaultOp::Reflink,
        Fault::Error(ErrorKind::PermissionDenied),
    ));
    let armed = fs
        .reflink_file(&src, &dir.path().join("armed-dst"))
        .expect_err("an armed FaultOp::Reflink must refuse the clone");
    assert_eq!(armed.kind(), ErrorKind::PermissionDenied);
}

#[test]
fn a_reflink_rule_matches_the_destination_path() {
    // The rule is matched against the DESTINATION, the file the clone would
    // create — matching the source would fire on the wrong file when a
    // checkpoint places one table and skips another.
    let dir = tempfile::tempdir().expect("tempdir");
    let matching_src = dir.path().join("wanted-src");
    let plain_src = dir.path().join("src");
    std::fs::write(&matching_src, b"payload").expect("write source");
    std::fs::write(&plain_src, b"payload").expect("write source");

    let fs = FaultFs::new(StdFs);
    fs.injector().arm(
        FaultRule::new(FaultOp::Reflink, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("wanted-dst"),
    );

    // The SOURCE matches the filter and the destination does not: the call must
    // reach the backend, so its outcome equals the bare backend's.
    let bare = StdFs.reflink_file(&matching_src, &dir.path().join("bare-other"));
    let elsewhere = fs.reflink_file(&matching_src, &dir.path().join("other"));
    assert_eq!(
        bare.is_ok(),
        elsewhere.is_ok(),
        "a destination outside the filter must not fire the rule, even when \
         the SOURCE matches it: bare {bare:?} vs wrapped {elsewhere:?}",
    );

    let matched = fs
        .reflink_file(&plain_src, &dir.path().join("wanted-dst"))
        .expect_err("the armed rule fires on the matching destination");
    assert_eq!(matched.kind(), ErrorKind::PermissionDenied);
}

/// The longest request [`DescriptorFs`] takes by descriptor, as the ring takes
/// none past `i32::MAX`: a smaller cap, so a test can reach it.
const DESCRIPTOR_READ_CAP: usize = 64;

/// What [`DescriptorFs`] refuses a whole batch for before reading any of it:
/// an offset that overflows, or a request by descriptor past the cap.
fn check_descriptor_batch(reqs: &[BlockRead<'_>]) -> io::Result<()> {
    for req in reqs {
        req.offset
            .checked_add(req.buf.filled() as u64)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "offset overflows"))?;
        if req.file.backing_fd().is_some()
            && req.buf.capacity() - req.buf.filled() > DESCRIPTOR_READ_CAP
        {
            return Err(io::Error::new(ErrorKind::InvalidInput, "request too long"));
        }
    }
    Ok(())
}

/// A backend whose files carry a descriptor and which, when `by_descriptor`,
/// reads its batches through that descriptor past `FsFile::read_at`, as the
/// ring does; otherwise it reads them through `read_at`, as the default does.
struct DescriptorFs {
    mem: MemFs,
    by_descriptor: bool,
    /// Its queue issues the reads submitted at any wait, a wait for none
    /// included, as the ring's does.
    eager_queue: bool,
    paths: spin::Mutex<Vec<(i32, PathBuf)>>,
}

impl DescriptorFs {
    fn new(by_descriptor: bool) -> Self {
        Self {
            mem: MemFs::new(),
            by_descriptor,
            eager_queue: false,
            paths: spin::Mutex::new(Vec::new()),
        }
    }

    /// Reads by descriptor, through a queue that issues at a wait for none.
    fn eager() -> Self {
        Self {
            eager_queue: true,
            ..Self::new(true)
        }
    }

    /// One request, read the way this backend reads a batch.
    fn read_one(&self, req: &mut BlockRead<'_>) -> io::Result<()> {
        let offset = req
            .offset
            .checked_add(req.buf.filled() as u64)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "offset overflows"))?;
        let dst = req.buf.unfilled_mut();
        let want = dst.len();
        let n = match req.file.backing_fd().filter(|_| self.by_descriptor) {
            Some(fd) => {
                let path = self
                    .paths
                    .lock()
                    .iter()
                    .find(|(open, _)| *open == fd)
                    .map(|(_, path)| path.clone())
                    .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "unknown descriptor"))?;
                self.mem
                    .open(&path, &FsOpenOptions::new().read(true))?
                    .read_at(dst, offset)?
            }
            None => req.file.read_at(dst, offset)?,
        };
        req.buf.advance(n);
        if n == want {
            Ok(())
        } else {
            Err(io::Error::new(ErrorKind::UnexpectedEof, "short read"))
        }
    }
}

/// The queue of a [`DescriptorFs`]: it reads what was submitted at a wait for
/// some, as the default queue does, or at any wait when the backend is eager,
/// as the ring's does.
struct DescriptorQueue<'a> {
    fs: &'a DescriptorFs,
    submitted: Vec<QueuedRead>,
}

impl ReadQueue for DescriptorQueue<'_> {
    fn submit(&mut self, read: QueuedRead) {
        self.submitted.push(read);
    }

    fn outstanding(&self) -> usize {
        self.submitted.len()
    }

    fn issues_at_wait_for_none(&self) -> bool {
        self.fs.eager_queue
    }

    fn wait(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        if min == 0 && !self.fs.eager_queue {
            return;
        }
        for mut read in core::mem::take(&mut self.submitted) {
            let result = {
                let mut req = BlockRead {
                    file: read.file.as_ref(),
                    offset: read.offset,
                    buf: crate::fs::BlockBuf::new(&mut read.buf[..]),
                };
                self.fs.read_one(&mut req)
            };
            on_done(ReadDone {
                tag: read.tag,
                buf: read.buf,
                result,
            });
        }
    }
}

struct DescriptorFile {
    inner: Box<dyn FsFile>,
    fd: i32,
}

impl Read for DescriptorFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for DescriptorFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for DescriptorFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for DescriptorFile {
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn backing_fd(&self) -> Option<i32> {
        Some(self.fd)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }
}

impl Fs for DescriptorFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let inner = self.mem.open(path, opts)?;
        let mut paths = self.paths.lock();
        let fd = 100 + i32::try_from(paths.len()).expect("few files");
        paths.push((fd, path.to_path_buf()));
        Ok(Box::new(DescriptorFile { inner, fd }))
    }

    fn reads_by_descriptor(&self) -> bool {
        self.by_descriptor
    }

    // Its requests by descriptor are what it refuses a batch for whole.
    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> io::Result<()> {
        if !self.by_descriptor {
            return Ok(());
        }
        let by_descriptor: Vec<&BlockRead<'_>> = reqs
            .iter()
            .filter(|req| req.file.backing_fd().is_some())
            .collect();
        for req in by_descriptor {
            check_descriptor_batch(core::slice::from_ref(req))?;
        }
        Ok(())
    }

    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        Box::new(DescriptorQueue {
            fs: self,
            submitted: Vec::new(),
        })
    }

    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        // The whole batch is validated before the first read, as a backend
        // reading by descriptor must.
        check_descriptor_batch(reqs)?;
        reqs.iter_mut().try_for_each(|req| self.read_one(req))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.mem.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.mem.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.mem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.mem.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.mem.rename(from, to)?;
        // A descriptor follows its file through a rename.
        for (_, path) in self.paths.lock().iter_mut() {
            if path == from {
                *path = to.to_path_buf();
            }
        }
        Ok(())
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.mem.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.mem.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.mem.exists(path)
    }
}

/// Writes `/d/f0` and `/d/f1`, 16 bytes each, through `fs`, and opens both
/// for reading through it.
fn two_readable(fs: &dyn Fs) -> io::Result<Vec<Box<dyn FsFile>>> {
    let mut files = Vec::new();
    for name in ["f0", "f1"] {
        write_file(fs, name)?.write_all(&[name.as_bytes()[1]; 16])?;
        files.push(fs.open(
            Path::new(&format!("/d/{name}")),
            &FsOpenOptions::new().read(true),
        )?);
    }
    Ok(files)
}

/// A backend that reads its batches through `read_at` hands each request to
/// `FaultFile::read_at`, which takes it through the read rules: the wrapper
/// must not take it through them a second time, or every read counts twice
/// and a `skip` / `times` rule is spent by the same request.
#[test]
fn a_batch_a_backend_reads_through_read_at_is_screened_once() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(false));
    let files = two_readable(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .skip(1)
            .once(),
    );
    let before = fs.injector().read_count();

    let mut bufs = [[0u8; 16]; 2];
    let [b0, b1] = &mut bufs;
    let mut reqs = [
        BlockRead {
            file: files[0].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b0),
        },
        BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b1),
        },
    ];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the second read is failed by the rule");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert_eq!(
        fs.injector().read_count() - before,
        2,
        "each read counted once"
    );
    assert!(reqs[0].buf.is_full(), "the first read went through");
    Ok(())
}

/// A backend that reads by descriptor is screened by the wrapper, once per
/// request, and a request the rules fail ends the batch: the ones before it
/// are read and handed over, it and the ones after it are not.
#[test]
fn a_batch_a_backend_reads_by_descriptor_is_screened_once_each() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();

    let mut bufs = [[0u8; 16]; 2];
    let [b0, b1] = &mut bufs;
    let mut reqs = [
        BlockRead {
            file: files[0].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b0),
        },
        BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b1),
        },
    ];
    let mut handed = Vec::new();
    let err = fs
        .read_blocks_batched_each(&mut reqs, &mut |at, req| {
            handed.push((at, req.buf.filled_bytes().to_vec()));
        })
        .expect_err("the rule fails the read of f1");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert_eq!(handed, vec![(0, vec![b'0'; 16])]);
    assert_eq!(
        fs.injector().read_count() - before,
        2,
        "each read counted once"
    );
    Ok(())
}

/// A handle's reads are matched against the path it was opened at whichever
/// way they are read: a rename after the open changes neither a direct
/// `read_at` nor a read the backend makes by the handle's descriptor.
#[test]
fn a_renamed_handle_is_matched_by_its_open_path_by_descriptor_too() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    fs.rename(Path::new("/d/f0"), Path::new("/d/g0"))?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("/d/f0")
            .once(),
    );

    let mut buf = [0u8; 16];
    let mut reqs = [BlockRead {
        file: files[0].as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut buf),
    }];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the rule names the path the handle was opened at");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}

/// A request on a file this wrapper did not open is still counted, and one
/// whose offset overflows is left for the backend to refuse.
#[test]
fn a_descriptor_batch_counts_a_foreign_file_and_leaves_an_overflow_to_the_backend() -> io::Result<()>
{
    let fs = FaultFs::new(DescriptorFs::new(true));
    two_readable(&fs)?;
    let foreign = fs
        .inner
        .open(Path::new("/d/f0"), &FsOpenOptions::new().read(true))?;
    let before = fs.injector().read_count();

    let mut buf = [0u8; 16];
    let mut reqs = [BlockRead {
        file: foreign.as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut buf),
    }];
    fs.read_blocks_batched(&mut reqs)?;
    assert!(reqs[0].buf.is_full());
    assert_eq!(fs.injector().read_count() - before, 1);

    let mut buf = [0u8; 16];
    let mut overflowing = crate::fs::BlockBuf::new(&mut buf);
    overflowing.append(&[0]);
    let mut reqs = [BlockRead {
        file: foreign.as_ref(),
        offset: u64::MAX,
        buf: overflowing,
    }];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the backend refuses an overflowing offset");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        fs.injector().read_count() - before,
        1,
        "the overflowing request is not screened"
    );
    Ok(())
}

/// A queued read the rules fail never reaches the backend's queue; it is
/// held until the next wait, which hands it back failed with the others.
#[test]
fn a_queued_read_the_rules_fail_is_handed_back_by_the_next_wait() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files: Vec<Arc<dyn FsFile>> = two_readable(&fs)?.into_iter().map(Arc::from).collect();
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );

    let mut queue = fs.read_queue();
    for (tag, file) in files.iter().enumerate() {
        queue.submit(QueuedRead {
            tag,
            file: Arc::clone(file),
            offset: 0,
            buf: vec![0; 16],
        });
    }
    assert_eq!(queue.outstanding(), 2);
    assert_eq!(queue.held(), 2, "neither finishes without a wait");

    let mut done = Vec::new();
    queue.wait(2, &mut |read| done.push(read));
    done.sort_by_key(|read| read.tag);
    assert_eq!(done.len(), 2);
    assert!(done[0].result.is_ok());
    assert_eq!(done[0].buf, vec![b'0'; 16]);
    assert_eq!(
        done[1].result.as_ref().map_err(io::Error::kind),
        Err::<&(), _>(ErrorKind::PermissionDenied),
    );
    assert_eq!(queue.outstanding(), 0);
    Ok(())
}

/// A request whose offset overflows fails the whole batch before any of it is
/// read, so the requests after it are not screened: no rule is spent on a
/// read that never happens, and a retry still meets it.
#[test]
fn a_batch_failed_by_an_overflow_spends_no_rule_on_the_requests_after_it() -> io::Result<()> {
    // The overflowing request first, then last: either way the batch is
    // refused whole, before any of it is read.
    for overflow_first in [true, false] {
        let fs = FaultFs::new(DescriptorFs::new(true));
        let files = two_readable(&fs)?;
        fs.injector().arm(
            FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
                .on_path("f1")
                .once(),
        );
        let before = fs.injector().read_count();

        let mut bufs = [[0u8; 16]; 2];
        let [b0, b1] = &mut bufs;
        let mut overflowing = crate::fs::BlockBuf::new(b0);
        overflowing.append(&[0]);
        let overflowing = BlockRead {
            file: files[0].as_ref(),
            offset: u64::MAX,
            buf: overflowing,
        };
        let valid = BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b1),
        };
        let mut reqs = if overflow_first {
            [overflowing, valid]
        } else {
            [valid, overflowing]
        };
        let err = fs
            .read_blocks_batched(&mut reqs)
            .expect_err("the backend refuses an overflowing offset");
        assert_eq!(
            err.kind(),
            ErrorKind::InvalidInput,
            "overflow first: {overflow_first}"
        );
        assert_eq!(
            fs.injector().read_count(),
            before,
            "nothing was screened (overflow first: {overflow_first})"
        );

        let mut buf = [0u8; 16];
        let mut retry = [BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(&mut buf),
        }];
        let err = fs
            .read_blocks_batched(&mut retry)
            .expect_err("the rule still fails the read of f1");
        assert_eq!(
            err.kind(),
            ErrorKind::PermissionDenied,
            "overflow first: {overflow_first}"
        );
    }
    Ok(())
}

/// A queued read is screened when a wait issues it, as the backend's queue
/// issues it: one never waited for spends no rule, and a rule armed after the
/// submit but before the wait applies to it.
#[test]
fn a_queued_read_is_screened_when_a_wait_issues_it() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files: Vec<Arc<dyn FsFile>> = two_readable(&fs)?.into_iter().map(Arc::from).collect();
    let read_of = |tag| QueuedRead {
        tag,
        file: Arc::clone(&files[1]),
        offset: 0,
        buf: vec![0; 16],
    };
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();

    // Submitted, never waited for: never read.
    let mut queue = fs.read_queue();
    queue.submit(read_of(0));
    drop(queue);
    assert_eq!(fs.injector().read_count(), before, "nothing was read");

    // The rule is still there for the read a wait issues.
    let mut queue = fs.read_queue();
    queue.submit(read_of(1));
    let mut done = Vec::new();
    queue.wait(1, &mut |read| done.push(read));
    assert_eq!(
        done.first()
            .map(|read| read.result.as_ref().map_err(io::Error::kind)),
        Some(Err::<&(), _>(ErrorKind::PermissionDenied)),
    );

    // A rule armed between the submit and the wait applies. The spent rule
    // is cleared first, since an exhausted rule still claims the reads it
    // matches: only the rule armed below is there when the wait runs.
    fs.injector().clear();
    let mut queue = fs.read_queue();
    queue.submit(read_of(2));
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Interrupted))
            .on_path("f1")
            .once(),
    );
    let mut done = Vec::new();
    queue.wait(1, &mut |read| done.push(read));
    assert_eq!(
        done.first()
            .map(|read| read.result.as_ref().map_err(io::Error::kind)),
        Some(Err::<&(), _>(ErrorKind::Interrupted)),
    );

    // A wait for none may leave a read held in the backend's queue: it
    // screens nothing, and the read stays held for a wait for some, which
    // screens and issues it.
    fs.injector().clear();
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();
    let mut queue = fs.read_queue();
    queue.submit(read_of(3));
    queue.wait(0, &mut |_| {});
    assert_eq!(
        fs.injector().read_count(),
        before,
        "a wait for none screened"
    );
    assert_eq!(queue.held(), 1, "the read is held for a wait for some");
    let mut done = Vec::new();
    queue.wait(1, &mut |read| done.push(read));
    assert_eq!(
        done.first()
            .map(|read| read.result.as_ref().map_err(io::Error::kind)),
        Some(Err::<&(), _>(ErrorKind::PermissionDenied)),
    );
    Ok(())
}

/// A wait for none issues the submitted reads of a queue that issues them at
/// such a wait, as the ring's does, so a caller polling several queues has
/// them all in flight: each read is screened as it is issued.
#[test]
fn a_wait_for_none_issues_the_reads_of_a_queue_that_issues_them() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::eager());
    let files: Vec<Arc<dyn FsFile>> = two_readable(&fs)?.into_iter().map(Arc::from).collect();
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );

    let mut queue = fs.read_queue();
    for (tag, file) in files.iter().enumerate() {
        queue.submit(QueuedRead {
            tag,
            file: Arc::clone(file),
            offset: 0,
            buf: vec![0; 16],
        });
    }
    let mut done = Vec::new();
    queue.wait(0, &mut |read| done.push(read));
    done.sort_by_key(|read| read.tag);
    assert_eq!(done.len(), 2, "both reads were issued");
    assert_eq!(done[0].buf, vec![b'0'; 16]);
    assert_eq!(
        done[1].result.as_ref().map_err(io::Error::kind),
        Err::<&(), _>(ErrorKind::PermissionDenied),
    );
    assert_eq!(queue.outstanding(), 0);
    Ok(())
}

/// A batch the backend refuses whole before reading any of it (a request too
/// long for a read by descriptor) spends no rule on the requests before that
/// one, and fails with the backend's refusal.
#[test]
fn a_batch_the_backend_refuses_whole_spends_no_rule() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();

    let mut valid = [0u8; 16];
    let mut too_long = [0u8; DESCRIPTOR_READ_CAP + 1];
    let mut reqs = [
        BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(&mut valid),
        },
        BlockRead {
            file: files[0].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(&mut too_long),
        },
    ];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the backend refuses the batch");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(fs.injector().read_count(), before, "nothing was screened");

    let mut buf = [0u8; 16];
    let mut retry = [BlockRead {
        file: files[1].as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut buf),
    }];
    let err = fs
        .read_blocks_batched(&mut retry)
        .expect_err("the rule still fails the read of f1");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}

/// A handle opened through the wrapper and read by another backend's batch
/// is still taken through the read rules: the other backend cannot reach the
/// handle's descriptor past them, and reads it through the handle.
#[test]
fn a_handle_read_by_another_backend_is_still_screened() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );

    let mut buf = [0u8; 16];
    let mut reqs = [BlockRead {
        file: files[1].as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut buf),
    }];
    let err = fs
        .inner
        .read_blocks_batched(&mut reqs)
        .expect_err("the rule fails the read the other backend makes");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}

/// A request with nothing to read is not read by the backend, so no rule
/// is spent on it, in a batch or in a queue: the rule still fails the next
/// real read.
#[test]
fn an_empty_request_spends_no_rule() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();

    let mut empty: [u8; 0] = [];
    let mut reqs = [BlockRead {
        file: files[1].as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut empty),
    }];
    fs.read_blocks_batched(&mut reqs)?;

    let file: Arc<dyn FsFile> =
        Arc::from(fs.open(Path::new("/d/f1"), &FsOpenOptions::new().read(true))?);
    let mut queue = fs.read_queue();
    queue.submit(QueuedRead {
        tag: 0,
        file: Arc::clone(&file),
        offset: 0,
        buf: Vec::new(),
    });
    let mut done = Vec::new();
    queue.wait(1, &mut |read| done.push(read));
    assert!(done.first().is_some_and(|read| read.result.is_ok()));
    assert_eq!(fs.injector().read_count(), before, "nothing was read");

    let mut buf = [0u8; 16];
    let mut reqs = [BlockRead {
        file: files[1].as_ref(),
        offset: 0,
        buf: crate::fs::BlockBuf::new(&mut buf),
    }];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the rule still fails the first real read");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}

/// The backend reads the requests that have a descriptor first and the rest
/// on their own after: an overflow among the rest does not stop the reads by
/// descriptor, which are screened as they are read.
#[test]
fn an_overflow_without_a_descriptor_leaves_the_descriptor_reads_screened() -> io::Result<()> {
    let fs = FaultFs::new(DescriptorFs::new(true));
    let files = two_readable(&fs)?;
    let plain = MemFs::new();
    plain.create_dir_all(Path::new("/p"))?;
    plain
        .open(
            Path::new("/p/f"),
            &FsOpenOptions::new().write(true).create(true),
        )?
        .write_all(&[0; 16])?;
    let undescribed = plain.open(Path::new("/p/f"), &FsOpenOptions::new().read(true))?;
    assert!(
        undescribed.backing_fd().is_none(),
        "self-check: no descriptor"
    );
    fs.injector().arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::PermissionDenied))
            .on_path("f1")
            .once(),
    );
    let before = fs.injector().read_count();

    let mut bufs = [[0u8; 16]; 2];
    let [b0, b1] = &mut bufs;
    let mut overflowing = crate::fs::BlockBuf::new(b1);
    overflowing.append(&[0]);
    let mut reqs = [
        BlockRead {
            file: files[1].as_ref(),
            offset: 0,
            buf: crate::fs::BlockBuf::new(b0),
        },
        BlockRead {
            file: undescribed.as_ref(),
            offset: u64::MAX,
            buf: overflowing,
        },
    ];
    let err = fs
        .read_blocks_batched(&mut reqs)
        .expect_err("the batch fails");
    assert_eq!(
        err.kind(),
        ErrorKind::PermissionDenied,
        "the read by descriptor was screened"
    );
    assert_eq!(fs.injector().read_count() - before, 1);
    Ok(())
}

#[test]
fn identity_probes_forward_to_inner_backend() {
    let mem = MemFs::new();
    let inner_id = mem.backend_id();
    let inner_caps = mem.capabilities(Path::new("/"));
    let fs = FaultFs::new(mem);

    assert_eq!(
        fs.backend_id(),
        inner_id,
        "backend_id must reflect the wrapped backend (cross-backend hard-link safety)"
    );
    assert_eq!(
        fs.capabilities(Path::new("/")),
        inner_caps,
        "capabilities must reflect the wrapped backend, not a wrapper default"
    );
}
