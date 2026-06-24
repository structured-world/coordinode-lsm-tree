use super::*;
use crate::fs::MemFs;
use crate::io::ErrorKind;
use std::io::{Read, Write};
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
