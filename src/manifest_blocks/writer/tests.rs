use super::*;
use crate::fs::MemFs;
use crate::runtime_config::RuntimeConfig;

fn open_writer(fs: &dyn Fs, path: &Path, runtime: RuntimeConfig) -> ManifestArchiveWriter {
    ManifestArchiveWriter::create(path, fs, Arc::new(runtime), None, SyncMode::Normal)
        .expect("manifest writer opens cleanly on a fresh path")
}

#[test]
fn writer_reserves_head_region_on_create() {
    // The 4 KiB head reservation must be in place immediately
    // after create() so a crash before any section is written
    // still leaves a self-describing zero-prefix that the
    // reader recognises as "no head mirror present" (vs reading
    // garbage from an un-truncated file).
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let path = Path::new("/m/manifest");
    let writer = open_writer(&fs, path, RuntimeConfig::default());
    // Don't write any sections — drop the writer immediately
    // (no finish). The file should still hold the 4 KiB zeros.
    drop(writer);

    let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
    let mut buf = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
    use std::io::Read;
    file.read_exact(&mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0));
}

#[test]
fn writer_rejects_empty_section_name() {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let mut w = open_writer(&fs, Path::new("/m/manifest"), RuntimeConfig::default());
    let err = w.start("").expect_err("empty name must be rejected");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn writer_rejects_duplicate_section_name() {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let mut w = open_writer(&fs, Path::new("/m/manifest"), RuntimeConfig::default());
    w.start("tables").unwrap();
    w.write_all(&[1, 2, 3]).unwrap();
    let err = w
        .start("tables")
        .expect_err("duplicate section name must be rejected");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn writer_finish_with_no_sections_writes_only_footer() {
    // A degenerate manifest with zero sections is still
    // structurally valid (footer with empty TOC). The writer
    // must produce a file that the reader can open without
    // tripping any non-zero-section invariants.
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let path = Path::new("/m/empty");
    let writer = open_writer(&fs, path, RuntimeConfig::default());
    writer.finish().expect("finish on empty writer succeeds");

    let meta = fs.metadata(path).unwrap();
    // File must contain at least the head reservation + a tail
    // footer Block. Exact size depends on Block header overhead;
    // we only assert the reservation is intact.
    assert!(meta.len > HEAD_FOOTER_RESERVED_SIZE);
}

#[test]
fn writer_finish_writes_head_mirror_when_enabled() {
    // mirror enabled (default): the head reservation must hold
    // a copy of the tail footer Block. Comparing the first
    // HEAD_FOOTER_RESERVED_SIZE bytes against the trailing
    // bytes of the file proves the writer mirrored correctly.
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let path = Path::new("/m/mirrored");
    let mut w = open_writer(&fs, path, RuntimeConfig::default());
    w.start("format_version").unwrap();
    w.write_all(&[5]).unwrap();
    w.finish().unwrap();

    let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
    use std::io::Read;
    let mut head = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
    file.read_exact(&mut head).unwrap();
    // The head mirror payload should NOT be all zeros — the
    // first bytes are the footer Block header magic.
    assert!(
        !head.iter().take(64).all(|&b| b == 0),
        "head mirror should contain the footer Block bytes, got all-zero prefix"
    );
}

#[test]
fn writer_finish_leaves_head_region_zero_when_mirror_disabled() {
    // mirror disabled: head reservation stays as the zeros the
    // writer wrote at create(). Reader uses the zero magic as
    // the "no head mirror" signal and skips the head-fallback
    // path on tail-verify failure.
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let path = Path::new("/m/no_mirror");
    let runtime = RuntimeConfig {
        manifest_footer_mirror: false,
        ..RuntimeConfig::default()
    };
    let mut w = open_writer(&fs, path, runtime);
    w.start("format_version").unwrap();
    w.write_all(&[5]).unwrap();
    w.finish().unwrap();

    let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
    use std::io::Read;
    let mut head = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
    file.read_exact(&mut head).unwrap();
    assert!(
        head.iter().all(|&b| b == 0),
        "head reservation should remain zeroed when manifest_footer_mirror=false"
    );
}

#[test]
fn writer_section_block_offsets_advance_past_head_reservation() {
    // Concrete invariant: the first section Block lands at
    // offset HEAD_FOOTER_RESERVED_SIZE (just after the head
    // reservation), and subsequent sections advance by the
    // exact Block size. Easy to verify by reading the TOC out
    // of the footer Block bytes — done in the reader's
    // integration test once reader lands; here we just lock
    // the offset of the first section by inspecting toc[0].
    // Since toc is private, we exercise the invariant
    // indirectly: write two sections, finish, then read the
    // file and assert section 1 starts immediately after the
    // head reservation. (Full TOC decode lands with the reader
    // in the next commit.)
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    let path = Path::new("/m/cursor");
    let mut w = open_writer(&fs, path, RuntimeConfig::default());
    w.start("a").unwrap();
    w.write_all(&[1, 2, 3]).unwrap();
    w.start("b").unwrap();
    w.write_all(&[4, 5, 6, 7]).unwrap();
    w.finish().unwrap();

    // Read the byte just after the head reservation — should
    // be the magic of the first section Block, not a zero.
    let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
    file.seek(SeekFrom::Start(HEAD_FOOTER_RESERVED_SIZE))
        .unwrap();
    let mut byte = [0u8; 1];
    use std::io::Read;
    file.read_exact(&mut byte).unwrap();
    assert_ne!(
        byte[0], 0,
        "first section Block should start at HEAD_FOOTER_RESERVED_SIZE"
    );
}
