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

// Removed: `write_current_for_version_rejects_corrupt_footer_size_hint`.
// The checkpoint write path now goes through
// `ManifestArchiveReader::open` (canonical CURRENT digest path),
// which has head-mirror fallback for a torn tail size hint —
// recovery succeeds instead of erroring, which is the correct
// behaviour. Tail / head-mirror bounds checks are covered by
// `manifest_blocks::reader::tests::reader_fails_when_tail_corrupt_and_no_mirror`
// and siblings.
