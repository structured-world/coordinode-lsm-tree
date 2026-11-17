//! A backend that leaves chosen syncs out, so the time a write path spends in
//! each kind of sync shows as the difference between two arms.
//!
//! [`SkipSyncFs`] hands every call to the backend it wraps, except a sync of a
//! kind it is told to skip, which it answers as done without issuing it. A
//! file whose syncs all go through is returned as the backend opened it, so
//! the arm that skips nothing writes exactly as the bare backend does.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use lsm_tree::fs::{
    BlockRead, FileHint, Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions,
    ReadBuf, ReadQueue, SyncMode, SyncTarget, WriteIoStats, WriteLifetime, WriteOrigin,
};
use lsm_tree::io;
use lsm_tree::write_budget::WriteBudget;

/// The syncs a [`SkipSyncFs`] leaves out.
#[derive(Clone, Copy)]
pub struct Skip {
    /// Syncs of the tables and blob files a flush writes.
    pub flush_files: bool,
    /// Syncs of the manifest: its snapshots and its edit log.
    pub manifest: bool,
    /// Syncs of every other file written: pool files, dictionaries, compaction
    /// output.
    pub other_files: bool,
    /// Syncs of directories and of whole devices.
    pub directories: bool,
}

impl Skip {
    /// No sync left out.
    pub const NONE: Self = Self {
        flush_files: false,
        manifest: false,
        other_files: false,
        directories: false,
    };

    /// Every sync left out.
    pub const ALL: Self = Self {
        flush_files: true,
        manifest: true,
        other_files: true,
        directories: true,
    };

    /// Whether a sync of a file written for `origin` is left out.
    fn file(self, origin: Option<WriteOrigin>) -> bool {
        match origin {
            Some(WriteOrigin::FlushTable | WriteOrigin::FlushBlob) => self.flush_files,
            Some(WriteOrigin::Manifest) => self.manifest,
            _ => self.other_files,
        }
    }
}

/// `inner`, with the syncs `skip` names left out.
pub struct SkipSyncFs {
    inner: Arc<dyn Fs>,
    skip: Skip,
}

impl SkipSyncFs {
    pub fn new(inner: Arc<dyn Fs>, skip: Skip) -> Self {
        Self { inner, skip }
    }
}

impl Fs for SkipSyncFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let file = self.inner.open(path, opts)?;
        let writes = opts.write || opts.append || opts.create || opts.create_new || opts.truncate;
        if !writes || !self.skip.file(opts.write_origin) {
            return Ok(file);
        }
        Ok(Box::new(UnsyncedFile { inner: file }))
    }

    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        self.inner.read_blocks_batched(reqs)
    }

    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> io::Result<()> {
        self.inner.read_blocks_batched_each(reqs, on_read)
    }

    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        self.inner.read_queue()
    }

    fn reads_by_descriptor(&self) -> bool {
        self.inner.reads_by_descriptor()
    }

    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> io::Result<()> {
        self.inner.check_descriptor_batch(reqs)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir(path)
    }

    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        self.inner.same_file(a, b)
    }

    fn read_link(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        self.inner.read_link(path)
    }

    fn symlink(&self, target: &Path, link: &Path, to_dir: bool) -> io::Result<()> {
        self.inner.symlink(target, link, to_dir)
    }

    fn symlink_to_dir(&self, path: &Path) -> io::Result<bool> {
        self.inner.symlink_to_dir(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        if self.skip.directories {
            return Ok(());
        }
        self.inner.sync_directory(path)
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        if self.skip.directories {
            return Ok(());
        }
        self.inner.sync_directory_with(path, mode)
    }

    fn sync_device(&self, path: &Path) -> io::Result<()> {
        if self.skip.directories {
            return Ok(());
        }
        self.inner.sync_device(path)
    }

    // The targets left in go to the backend as one batch, so the arm that
    // skips nothing syncs them as the bare backend does. A skipped file is
    // not opened either: an error opening it does not surface.
    fn sync_many(&self, targets: &[SyncTarget<'_>]) -> io::Result<()> {
        let kept: Vec<SyncTarget<'_>> = targets
            .iter()
            .copied()
            .filter(|target| match target {
                SyncTarget::FileData { origin, .. } => !self.skip.file(Some(*origin)),
                SyncTarget::Folder { .. } => !self.skip.directories,
            })
            .collect();
        if kept.is_empty() {
            return Ok(());
        }
        self.inner.sync_many(&kept)
    }

    fn idles_between_io_queues(&self, path: &Path) -> bool {
        self.inner.idles_between_io_queues(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.hard_link(src, dst)
    }

    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }

    fn volume_id(&self, path: &Path) -> Option<u64> {
        self.inner.volume_id(path)
    }

    fn capabilities(&self, path: &Path) -> FsCapabilities {
        self.inner.capabilities(path)
    }

    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        self.inner.try_disable_cow(path)
    }

    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.reflink_file(src, dst)
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.inner.truncate_file(path)
    }

    fn write_io_stats(&self) -> Option<&Arc<WriteIoStats>> {
        self.inner.write_io_stats()
    }

    fn write_budget(&self) -> Option<&Arc<WriteBudget>> {
        self.inner.write_budget()
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        self.inner.available_space(path)
    }

    fn allocated_size(&self, path: &Path) -> io::Result<Option<u64>> {
        self.inner.allocated_size(path)
    }

    fn extent_is_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_is_hole(path, offset, len)
    }

    fn extent_contains_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_contains_hole(path, offset, len)
    }
}

/// A file whose syncs are answered as done without being issued.
struct UnsyncedFile {
    inner: Box<dyn FsFile>,
}

impl std::io::Read for UnsyncedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl std::io::Write for UnsyncedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        self.inner.write_vectored(bufs)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for UnsyncedFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for UnsyncedFile {
    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_all_with(&self, _mode: SyncMode) -> io::Result<()> {
        Ok(())
    }

    fn sync_data_with(&self, _mode: SyncMode) -> io::Result<()> {
        Ok(())
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn hard_link_count(&self) -> io::Result<u64> {
        self.inner.hard_link_count()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn read_many(&self, regions: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        self.inner.read_many(regions)
    }

    fn read_at_buf(&self, buf: &mut ReadBuf<'_>, offset: u64) -> io::Result<()> {
        self.inner.read_at_buf(buf, offset)
    }

    fn read_many_buf(&self, regions: &mut [(u64, ReadBuf<'_>)]) -> io::Result<()> {
        self.inner.read_many_buf(regions)
    }

    fn backing_fd(&self) -> Option<i32> {
        self.inner.backing_fd()
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        self.inner.try_lock_exclusive()
    }

    fn hint(&self, hint: FileHint) -> io::Result<()> {
        self.inner.hint(hint)
    }

    fn start_writeback(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.start_writeback(offset, len)
    }

    fn set_write_lifetime(&self, lifetime: WriteLifetime) -> io::Result<()> {
        self.inner.set_write_lifetime(lifetime)
    }
}
