use super::*;

/// A `no_wait` open leaves the file it opened blocking: `O_NONBLOCK` is the
/// open's alone, and a file left with it fails `io_uring` writes that would
/// block.
#[test]
fn a_no_wait_open_leaves_its_file_blocking() -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file");
    std::fs::write(&path, b"")?;
    let opts = FsOpenOptions::new().write(true).no_wait(true);
    let mut builder = std::fs::OpenOptions::new();
    builder.write(true);
    apply_open_flags(&mut builder, &opts);
    let file = builder.open(&path)?;
    // SAFETY: the descriptor `file` owns; `F_GETFL` takes no pointer.
    let opened = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert_ne!(opened & libc::O_NONBLOCK, 0, "self-check: the open set it");

    finish_open(&file, &opts)?;
    // SAFETY: as above.
    let after = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(after & libc::O_NONBLOCK, 0);
    Ok(())
}
