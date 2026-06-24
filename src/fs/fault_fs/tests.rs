use super::*;
use crate::fs::MemFs;
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
