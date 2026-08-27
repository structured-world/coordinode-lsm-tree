use super::*;
use crate::fs::{FsOpenOptions, MemFs, StdFs};
use std::io::{Read, Write};

/// `link_or_copy_cross_fs` must transparently stream bytes through
/// both trait objects when source and destination back ends differ
/// (here: `StdFs` source vs. `MemFs` target — the `MemFs` backend
/// has no way to see the on-disk source file, so the hard-link
/// attempt returns `NotFound` and we fall through to a streamed
/// copy). Verifies BOTH the copy lands AND the two filesystems
/// stay independent under subsequent mutation.
#[test]
fn cross_fs_link_or_copy_streams_through_trait() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("payload.bin");
    std::fs::write(&src, b"cross-fs-payload").unwrap();

    let std_fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mem_fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    mem_fs.create_dir_all(Path::new("/dst")).unwrap();

    let dst = Path::new("/dst/payload.bin");
    // use_reflink = true, but src/dst are different backends (StdFs vs
    // MemFs) so the reflink + hard_link paths are both gated out by the
    // shared-namespace check, exercising the cross-fs streamed copy.
    let bytes = link_or_copy_cross_fs(&std_fs, &src, &mem_fs, dst, SyncMode::Normal, true).unwrap();
    assert_eq!(bytes, b"cross-fs-payload".len() as u64);

    // Bytes landed in MemFs.
    let mut buf = String::new();
    mem_fs
        .open(dst, &FsOpenOptions::new().read(true))
        .unwrap()
        .read_to_string(&mut buf)
        .unwrap();
    assert_eq!(buf, "cross-fs-payload");

    // Mutating `dst` via MemFs must NOT affect the StdFs source —
    // proves the streamed copy produced an independent file rather
    // than aliasing.
    let mut writer = mem_fs
        .open(dst, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    writer.write_all(b"mutated-via-mem-fs").unwrap();
    drop(writer);

    assert_eq!(std::fs::read(&src).unwrap(), b"cross-fs-payload");

    let mut after = String::new();
    mem_fs
        .open(dst, &FsOpenOptions::new().read(true))
        .unwrap()
        .read_to_string(&mut after)
        .unwrap();
    assert_eq!(after, "mutated-via-mem-fs");
}

/// The checkpoint records a restricted table's recovery bound from the CAPTURED
/// version view (`restrict_lower_bound`), NOT by re-reading the live
/// `.restrict-bound` sidecar file — a concurrent tight-space compaction can be
/// rewriting that file for the same SST id. Proven by leaving a STALE live sidecar
/// (a DIFFERENT bound) beside the source SST: the checkpoint's sidecar must encode
/// the captured bound, never the stale file's. Under the pre-fix copy-the-file
/// logic the checkpoint carried the stale bound instead.
#[test]
fn checkpoint_binds_restrict_sidecar_to_captured_view_not_live_file() -> crate::Result<()> {
    use crate::blob_tree::FragmentationMap;
    use crate::cache::Cache;
    use crate::descriptor_table::DescriptorTable;
    use crate::table::{Table, Writer};
    use crate::version::{BlobFileList, Level, Run, Version};
    use crate::{InternalValue, TreeType, ValueType};

    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let src_tables = src_dir.path().join(TABLES_FOLDER);
    fs.create_dir_all(&src_tables)?;
    let sst = src_tables.join("0");

    // A multi-block SST so a restriction bound lands on a real block boundary.
    let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = w.finish()?.expect("table written");

    // Recover, then build the restricted VIEW at k00100 (no punch needed here:
    // reopen_restricted reads the live suffix digest off the whole file).
    let table = {
        let mut params = crate::table::RecoverParams::new(
            sst.clone(),
            checksum,
            0,
            Arc::clone(&fs),
            crate::comparator::default_comparator(),
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
        );
        params.descriptor_table = Some(Arc::new(DescriptorTable::new(10)));
        Table::recover(params)?
    };
    let restricted = table.reopen_restricted(b"k00100".to_vec().into())?;

    // A STALE live sidecar with a DIFFERENT bound beside the SOURCE SST: the copy
    // path would carry this, the captured-bound path must ignore it.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00050", SyncMode::Normal)?;

    // Build a one-table version and checkpoint (link) it into dst.
    let run = Arc::new(Run::new(vec![restricted]).expect("non-empty run"));
    let version = Version::from_levels(
        1,
        TreeType::Standard,
        vec![Level::from_runs(vec![run])],
        BlobFileList::default(),
        FragmentationMap::default(),
    );
    let dst_root = dst_dir.path();
    fs.create_dir_all(&dst_root.join(TABLES_FOLDER))?;
    link_tables(&version, dst_root, &fs, SyncMode::Normal, false)?;

    // The checkpoint's sidecar encodes the CAPTURED bound (k00100), not the stale
    // live file's (k00050).
    let dst_sst = dst_root.join(TABLES_FOLDER).join("0");
    match crate::restrict_bound::read(&*fs, &dst_sst, None)? {
        crate::restrict_bound::SidecarRead::Present(id, bound) => {
            assert_eq!(id, 0, "the sidecar binds the table id");
            assert_eq!(
                bound, b"k00100",
                "checkpoint must record the captured bound, not the stale live file's",
            );
        }
        _ => {
            panic!("checkpoint must write a valid restrict-bound sidecar for the restricted table")
        }
    }
    Ok(())
}

// Removed: `write_current_for_version_rejects_corrupt_footer_size_hint`.
// The checkpoint write path now goes through
// `ManifestArchiveReader::open` (canonical CURRENT digest path),
// which has head-mirror fallback for a torn tail size hint —
// recovery succeeds instead of erroring, which is the correct
// behaviour. Tail / head-mirror bounds checks are covered by
// `manifest_blocks::reader::tests::reader_fails_when_tail_corrupt_and_no_mirror`
// and siblings.

/// A [`Fs`] over `StdFs` that ADVERTISES reflink support and implements
/// `reflink_file` as a plain byte copy (the semantics a real clone gives the
/// caller: an independent destination inode). Used to drive the checkpoint's
/// reflink fast path on any host filesystem.
mod reflink_fs {
    use crate::fs::{Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
    use crate::io;
    use std::path::Path;

    pub(super) struct ReflinkFs;

    impl Fs for ReflinkFs {
        fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
            StdFs.open(path, opts)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.create_dir_all(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
            StdFs.read_dir(path)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_file(path)
        }
        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            StdFs.rename(from, to)
        }
        fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
            StdFs.metadata(path)
        }
        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            StdFs.sync_directory(path)
        }
        fn exists(&self, path: &Path) -> io::Result<bool> {
            StdFs.exists(path)
        }
        fn backend_id(&self) -> Option<u64> {
            StdFs.backend_id()
        }
        fn volume_id(&self, path: &Path) -> Option<u64> {
            StdFs.volume_id(path)
        }
        fn capabilities(&self, path: &Path) -> FsCapabilities {
            FsCapabilities {
                reflink: true,
                ..StdFs.capabilities(path)
            }
        }
        fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
            let bytes = std::fs::read(src)?;
            std::fs::write(dst, bytes)?;
            Ok(())
        }
    }
}

/// A [`Fs`] over [`reflink_fs::ReflinkFs`] that models Windows handle
/// semantics: flushing a handle opened WITHOUT write access fails with
/// `ERROR_ACCESS_DENIED` (surfaced as `PermissionDenied`). Conforming custom
/// backends may behave the same way, so the checkpoint must open a
/// destination it intends to sync with write access.
mod windows_reflink_fs {
    use crate::fs::{Fs, FsFile, FsMetadata, FsOpenOptions};
    use crate::io;
    use std::path::Path;

    pub(super) struct WindowsReflinkFs;

    impl Fs for WindowsReflinkFs {
        fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
            let file = super::reflink_fs::ReflinkFs.open(path, opts)?;
            if opts.write {
                Ok(file)
            } else {
                Ok(Box::new(FlushRefusingFile(file)))
            }
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.create_dir_all(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<Vec<crate::fs::FsDirEntry>> {
            super::reflink_fs::ReflinkFs.read_dir(path)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.remove_file(path)
        }
        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.remove_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.rename(from, to)
        }
        fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
            super::reflink_fs::ReflinkFs.metadata(path)
        }
        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.sync_directory(path)
        }
        fn exists(&self, path: &Path) -> io::Result<bool> {
            super::reflink_fs::ReflinkFs.exists(path)
        }
        fn backend_id(&self) -> Option<u64> {
            super::reflink_fs::ReflinkFs.backend_id()
        }
        fn volume_id(&self, path: &Path) -> Option<u64> {
            super::reflink_fs::ReflinkFs.volume_id(path)
        }
        fn capabilities(&self, path: &Path) -> crate::fs::FsCapabilities {
            super::reflink_fs::ReflinkFs.capabilities(path)
        }
        fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
            super::reflink_fs::ReflinkFs.reflink_file(src, dst)
        }
    }

    /// A read-only handle that refuses to flush, the way a Windows handle
    /// opened without `GENERIC_WRITE` does.
    struct FlushRefusingFile(Box<dyn FsFile>);

    impl std::io::Read for FlushRefusingFile {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(buf)
        }
    }
    impl std::io::Write for FlushRefusingFile {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.flush()
        }
    }
    impl std::io::Seek for FlushRefusingFile {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.0.seek(pos)
        }
    }
    impl FsFile for FlushRefusingFile {
        fn sync_all(&self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "flush of a read-only handle",
            ))
        }
        fn sync_data(&self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "flush of a read-only handle",
            ))
        }
        fn metadata(&self) -> io::Result<FsMetadata> {
            self.0.metadata()
        }
        fn set_len(&self, size: u64) -> io::Result<()> {
            self.0.set_len(size)
        }
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.0.read_at(buf, offset)
        }
        fn lock_exclusive(&self) -> io::Result<()> {
            self.0.lock_exclusive()
        }
    }
}

/// The reflink fast path syncs the freshly cloned destination through a handle
/// it opens itself — and that handle must carry WRITE access, because Windows
/// (and conforming custom backends) refuse to flush a read-only handle with
/// `ERROR_ACCESS_DENIED`. A build that opens the destination read-only fails
/// every reflink checkpoint on such a backend right after a successful clone.
#[test]
fn reflink_checkpoint_sync_handle_carries_write_access() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("payload.bin");
    std::fs::write(&src, b"reflink-payload").unwrap();
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&dst_dir).unwrap();
    let dst = dst_dir.join("payload.bin");

    let fs: Arc<dyn Fs> = Arc::new(windows_reflink_fs::WindowsReflinkFs);
    let result = link_or_copy_cross_fs(&fs, &src, &fs, &dst, SyncMode::Full, true);
    assert_eq!(
        result.expect("the clone's sync handle must be opened with write access"),
        b"reflink-payload".len() as u64,
    );
}

/// The reflink fast path must SYNC the cloned destination file before the
/// checkpoint treats it as complete. The streamed-copy fallback already does;
/// the real Linux / macOS `reflink_file` implementations never sync, and the
/// surrounding checkpoint code syncs only DIRECTORIES. Without the file sync a
/// power loss can leave the checkpoint's manifest and directory entries durable
/// while the cloned extents are not. Faulting the file sync proves it happens:
/// a build that skips it returns `Ok` and never sees the fault.
#[test]
fn reflink_checkpoint_copy_syncs_the_destination_file() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("payload.bin");
    std::fs::write(&src, b"reflink-payload").unwrap();
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&dst_dir).unwrap();
    let dst = dst_dir.join("payload.bin");

    let fault = FaultFs::new(reflink_fs::ReflinkFs);
    fault.injector().arm(FaultRule::new(
        FaultOp::SyncAll,
        Fault::Error(ErrorKind::Other),
    ));
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let result = link_or_copy_cross_fs(&fs, &src, &fs, &dst, SyncMode::Full, true);
    assert!(
        result.is_err(),
        "the reflinked destination must be synced at the requested durability \
         before the clone counts as complete; got {result:?}",
    );
}
