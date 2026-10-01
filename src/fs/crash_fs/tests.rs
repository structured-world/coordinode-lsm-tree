use super::*;
use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs};
use crate::io::ErrorKind;
use std::io::{Read, Seek, SeekFrom, Write};
use test_log::test;

/// Reads the full content of `path` through `fs`.
fn read(fs: &dyn Fs, path: &str) -> Vec<u8> {
    let mut file = fs
        .open(Path::new(path), &FsOpenOptions::new().read(true))
        .expect("open for read");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).expect("read_to_end");
    buf
}

/// With directory entries tracked, a new file's entry is durable only once
/// its directory is synced: a file whose content was synced but whose
/// directory never was is lost, and a sync of another directory does not
/// save it.
#[test]
fn a_synced_file_in_an_unsynced_directory_is_lost() {
    let fs = CrashFs::new(MemFs::new()).tracking_directory_entries();
    fs.create_dir_all(Path::new("/d")).unwrap();
    fs.create_dir_all(Path::new("/e")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"synced").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(Path::new("/e")).unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/a")).unwrap(),
        "a file whose directory entry was never synced does not survive a crash"
    );
}

/// With directory entries tracked, a rename's new name is an entry of its
/// own: without a sync of its directory it is lost.
#[test]
fn a_rename_without_a_directory_sync_is_lost() {
    let fs = CrashFs::new(MemFs::new()).tracking_directory_entries();
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(Path::new("/d")).unwrap();
    fs.rename(Path::new("/d/src"), Path::new("/d/dst")).unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/dst")).unwrap(),
        "a renamed-to name whose directory was not synced does not survive a crash"
    );
}

/// Every operation that makes or removes a directory entry, every write that
/// resolves the entry it lands on, and a directory sync, runs with the
/// namespace held, in either mode, so none lands inside another: not a write
/// to a file other than the one it resolved, not a creation between a sync's
/// backend call and what the sync is credited with, not a removal between an
/// open's existence probe and the open, not a new entry at a renamed-away
/// path before the rename checks it. Each waits while another holds the
/// namespace.
#[test]
fn namespace_operations_wait_for_one_in_flight() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    for name in ["/d/gone", "/d/src", "/d/linked", "/d/written"] {
        let mut f = fs
            .open(
                Path::new(name),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"x").unwrap();
    }

    type Operation = (&'static str, fn(&CrashFs));
    let operations: [Operation; 9] = [
        ("directory sync", |fs| {
            fs.sync_directory(Path::new("/d")).unwrap();
        }),
        ("create", |fs| {
            fs.open(
                Path::new("/d/new"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        }),
        ("remove", |fs| fs.remove_file(Path::new("/d/gone")).unwrap()),
        ("rename", |fs| {
            fs.rename(Path::new("/d/src"), Path::new("/d/moved"))
                .unwrap();
        }),
        ("hard link", |fs| {
            fs.hard_link(Path::new("/d/linked"), Path::new("/d/link"))
                .unwrap();
        }),
        // A write resolves the entry it lands on before the backend follows
        // the same path: a symlink replaced in between would leave the
        // simulator tracking one file while the bytes went to another.
        ("write", |fs| {
            fs.open(Path::new("/d/written"), &FsOpenOptions::new().write(true))
                .unwrap();
        }),
        ("punch hole", |fs| {
            fs.punch_hole(Path::new("/d/written"), 0, 1).unwrap();
        }),
        ("truncate", |fs| {
            fs.truncate_file(Path::new("/d/written")).unwrap();
        }),
        ("directory removal", |fs| {
            fs.remove_dir_all(Path::new("/d")).unwrap();
        }),
    ];
    for (name, operation) in operations {
        let held = fs.namespace.lock();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = {
            let fs = fs.clone();
            std::thread::spawn(move || {
                operation(&fs);
                done_tx.send(()).unwrap();
            })
        };
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "{name} must wait while the namespace is held"
        );
        drop(held);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("{name} proceeds once the namespace is free"));
        worker.join().unwrap();
    }
}

/// A rename onto its own path changes nothing on disk (POSIX rename(2): same
/// file, no-op), so a file that was durable before it stays durable.
#[test]
fn a_rename_onto_itself_keeps_a_durable_file() {
    let fs = CrashFs::new(MemFs::new()).tracking_directory_entries();
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(Path::new("/d")).unwrap();
    fs.rename(Path::new("/d/a"), Path::new("/d/a")).unwrap();

    fs.crash();
    assert_eq!(read(&fs, "/d/a"), b"durable");
}

/// A rename between two hard links of one file is a no-op on a POSIX
/// filesystem (rename(2): both names refer to the same file), so both durable
/// names survive a crash.
#[cfg(unix)]
#[test]
fn a_rename_between_links_of_one_file_keeps_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    let src = dir.path().join("src");
    let link = dir.path().join("link");

    let mut f = fs
        .open(&src, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.hard_link(&src, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    fs.rename(&link, &src).unwrap();
    assert!(
        fs.exists(&link).unwrap(),
        "the backend treated the rename as a no-op"
    );

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&fs, link.to_str().unwrap()), b"durable");
}

/// A rename between two symlinks to one file moves the source entry over the
/// destination, as rename(2) acts on the entries and not on what they point
/// to: the destination is a new entry, lost on a crash before its directory
/// is synced.
#[cfg(unix)]
#[test]
fn a_rename_between_symlinks_to_one_file_moves_the_entry() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let target = dir.path().join("target");
    let a = dir.path().join("a");
    let b = dir.path().join("b");

    let mut f = fs
        .open(&target, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    drop(f);
    std::os::unix::fs::symlink(&target, &a).unwrap();
    std::os::unix::fs::symlink(&target, &b).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    fs.rename(&a, &b).unwrap();
    assert!(!fs.exists(&a).unwrap(), "the source entry was moved");

    fs.crash();
    assert!(
        !b.exists(),
        "a renamed-to entry whose directory was not synced does not survive a crash"
    );
    assert_eq!(read(&fs, target.to_str().unwrap()), b"durable");
}

/// Two hard links of one file share its bytes, so a sync through one name
/// makes them durable under the other: with the synced name removed, a crash
/// leaves the other name holding the synced bytes, not the ones it was linked
/// with.
#[cfg(any(unix, windows))]
#[test]
fn a_sync_through_one_hard_link_is_durable_through_the_other() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    let src = dir.path().join("src");
    let link = dir.path().join("link");

    let mut f = fs
        .open(&src, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"v1").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.hard_link(&src, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    let mut f = fs
        .open(&link, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    f.write_all(b"v2").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.remove_file(&link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"v2");
}

/// A pre-existing file linked to a new name keeps its durable bytes under its
/// own name: a write through it that is never synced is rolled back on a
/// crash, not taken for a new file and removed.
#[cfg(any(unix, windows))]
#[test]
fn a_linked_pre_existing_file_keeps_its_durable_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let link = dir.path().join("link");
    std::fs::write(&src, b"durable").unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    fs.hard_link(&src, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    let mut f = fs
        .open(&src, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&fs, link.to_str().unwrap()), b"durable");
}

/// Removing one name of a hard-linked file keeps what the file went through:
/// an unsynced write made through the removed name is still unsynced under
/// the surviving one, so a later write through it does not take those bytes
/// for its baseline, and a crash rolls them back.
#[cfg(any(unix, windows))]
#[test]
fn removing_a_hard_link_keeps_the_file_touched() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let link = dir.path().join("link");
    std::fs::write(&src, b"durable").unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    fs.hard_link(&src, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    let mut f = fs
        .open(&link, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);
    fs.remove_file(&link).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    // A write through the surviving name, never synced either.
    let f = fs
        .open(&src, &FsOpenOptions::new().write(true).append(true))
        .unwrap();
    drop(f);

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
}

/// Renaming another file over one name of a hard-linked file replaces that
/// name only: an unsynced write the file got through it is still unsynced
/// under the surviving name, so a crash rolls it back there.
#[cfg(any(unix, windows))]
#[test]
fn renaming_over_a_hard_link_keeps_the_file_touched() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let link = dir.path().join("link");
    let other = dir.path().join("other");
    std::fs::write(&src, b"durable").unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    fs.hard_link(&src, &link).unwrap();
    let mut f = fs
        .open(&other, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"other").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(dir.path()).unwrap();

    let mut f = fs
        .open(&link, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);
    fs.rename(&other, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    // A write through the surviving name, never synced either.
    let f = fs
        .open(&src, &FsOpenOptions::new().write(true).append(true))
        .unwrap();
    drop(f);

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&fs, link.to_str().unwrap()), b"other");
}

/// A hard link of a symlink is a second name of the symlink, not of the file
/// it points to (Linux `linkat(2)` without `AT_SYMLINK_FOLLOW`): the two
/// names hold no bytes of their own, so no durable image is kept under them.
#[cfg(target_os = "linux")]
#[test]
fn a_hard_link_of_a_symlink_keeps_no_image_under_the_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let link = dir.path().join("link");
    let alias = dir.path().join("alias");
    let fs = CrashFs::new(crate::fs::StdFs);
    let mut f = fs
        .open(&target, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"v1").unwrap();
    f.sync_all().unwrap();
    drop(f);
    std::os::unix::fs::symlink("target", &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    fs.hard_link(&link, &alias).unwrap();
    let state = fs.state.lock();
    assert!(
        !state.durable.contains_key(&link) && !state.durable.contains_key(&alias),
        "no image is kept under a symlink's names"
    );
    assert!(
        state.link_group.is_empty(),
        "the symlink's names are not grouped"
    );
}

/// A write follows as many symlinks as Linux does, `MAX_SYMLINKS`, and the
/// chain ends at the entry past the last of them; one more link than that is
/// refused, as the kernel refuses it with `ELOOP`.
#[cfg(unix)]
#[test]
fn a_write_follows_as_many_symlinks_as_linux_and_no_more() {
    let dir = tempfile::tempdir().unwrap();
    // Resolved, so that a symlink above the temporary directory (`/var` on
    // macOS) does not count towards the chain.
    let base = std::fs::canonicalize(dir.path()).unwrap();
    let fs = CrashFs::new(crate::fs::StdFs);
    let target = base.join("target");
    // link{i} -> link{i+1}, the last of them -> target.
    let chain = |links: usize| -> std::path::PathBuf {
        for i in 0..links {
            let next = if i + 1 == links {
                String::from("target")
            } else {
                format!("link{}-{}", links, i + 1)
            };
            std::os::unix::fs::symlink(next, base.join(format!("link{links}-{i}"))).unwrap();
        }
        base.join(format!("link{links}-0"))
    };

    let at_the_limit = chain(MAX_SYMLINKS);
    assert_eq!(fs.entry_of(&at_the_limit).unwrap(), target);
    let past_the_limit = chain(MAX_SYMLINKS + 1);
    let error = fs.entry_of(&past_the_limit).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

/// An open that may only create (`create_new`, `O_EXCL`) does not follow a
/// final symlink: the backend refuses an existing symlink with
/// `AlreadyExists`, whatever the link points to, a cycle included, and the
/// simulator gives that answer, not one of its own.
#[cfg(unix)]
#[test]
fn a_create_new_open_of_a_symlink_is_refused_as_existing() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    std::os::unix::fs::symlink("b", &a).unwrap();
    std::os::unix::fs::symlink("a", &b).unwrap();

    // With and without the directory-entry model.
    for fs in [
        CrashFs::new(crate::fs::StdFs),
        CrashFs::new(crate::fs::StdFs).tracking_directory_entries(),
    ] {
        let error = fs
            .open(&a, &FsOpenOptions::new().write(true).create_new(true))
            .err()
            .expect("an existing name is refused");
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    }
}

/// Hard-linking a dangling symlink links the symlink itself (Linux
/// `linkat(2)` without `AT_SYMLINK_FOLLOW`): the link the backend made is
/// reported as made, though no file stands behind it to compare.
#[cfg(target_os = "linux")]
#[test]
fn hard_linking_a_dangling_symlink_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let link = dir.path().join("link");
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink("missing", &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    fs.hard_link(&link, &alias).unwrap();
    assert!(
        std::fs::symlink_metadata(&alias).is_ok(),
        "the backend made it"
    );
}

/// A rename between two hard links of one dangling symlink changes nothing
/// (rename(2): both names refer to the same file), though the link points
/// nowhere: both durable names survive a crash.
#[cfg(target_os = "linux")]
#[test]
fn a_rename_between_links_of_a_dangling_symlink_keeps_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let link = dir.path().join("link");
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink("missing", &link).unwrap();
    fs.hard_link(&link, &alias).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    fs.rename(&alias, &link).unwrap();
    assert!(
        std::fs::symlink_metadata(&alias).is_ok(),
        "the backend treated the rename as a no-op"
    );

    fs.crash();
    assert!(std::fs::symlink_metadata(&link).is_ok());
    assert!(std::fs::symlink_metadata(&alias).is_ok());
}

/// An open that creates through a dangling symlink makes the symlink's
/// target, not the symlink: the target is the new entry, lost on a crash
/// before its directory is synced, and the symlink, already durable, stays.
#[cfg(unix)]
#[test]
fn a_create_through_a_dangling_symlink_makes_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let link = dir.path().join("link");
    let target = dir.path().join("target");
    std::os::unix::fs::symlink("target", &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();

    let mut f = fs
        .open(&link, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert!(target.exists(), "the open created the target");

    fs.crash();
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "the durable symlink survives"
    );
    assert!(
        !target.exists(),
        "a created entry whose directory was not synced does not survive a crash"
    );

    // Once the directory is synced, the created target is durable.
    let mut f = fs
        .open(&link, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(dir.path()).unwrap();
    fs.crash();
    assert_eq!(read(&fs, target.to_str().unwrap()), b"data");
}

/// A write through a symlink is a write of the file it points to, in either
/// mode: an unsynced write through the link is not taken for the baseline by
/// a later write through the file's own name, and a crash rolls it back.
#[cfg(unix)]
#[test]
fn a_write_through_a_symlink_is_a_write_of_its_target() {
    for tracking in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("link");
        std::fs::write(&target, b"v1").unwrap();
        std::os::unix::fs::symlink("target", &link).unwrap();
        let fs = CrashFs::new(crate::fs::StdFs);
        let fs = if tracking {
            fs.tracking_directory_entries()
        } else {
            fs
        };

        let mut f = fs
            .open(&link, &FsOpenOptions::new().write(true).truncate(true))
            .unwrap();
        f.write_all(b"v2").unwrap();
        drop(f);
        let f = fs
            .open(&target, &FsOpenOptions::new().write(true).append(true))
            .unwrap();
        drop(f);

        fs.crash();
        assert_eq!(
            read(&fs, target.to_str().unwrap()),
            b"v1",
            "tracking: {tracking}"
        );
    }
}

/// A file named through a symlinked directory is the file named through the
/// real one, in either mode: an entry made through either spelling is made
/// durable by a sync of the directory under either, and an unsynced write
/// through one is not taken for the baseline by a later write through the
/// other.
#[cfg(unix)]
#[test]
fn a_symlinked_directory_names_the_files_of_the_real_one() {
    for tracking in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(dir.path()).unwrap();
        let real = base.join("real");
        let alias = base.join("alias");
        std::fs::create_dir(&real).unwrap();
        std::os::unix::fs::symlink("real", &alias).unwrap();
        std::fs::write(real.join("old"), b"v1").unwrap();
        let fs = CrashFs::new(crate::fs::StdFs);
        let fs = if tracking {
            fs.tracking_directory_entries()
        } else {
            fs
        };

        let mut f = fs
            .open(
                &alias.join("new"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"data").unwrap();
        f.sync_all().unwrap();
        drop(f);
        fs.sync_directory(&real).unwrap();

        let mut f = fs
            .open(
                &alias.join("old"),
                &FsOpenOptions::new().write(true).truncate(true),
            )
            .unwrap();
        f.write_all(b"v2").unwrap();
        drop(f);
        let f = fs
            .open(
                &real.join("old"),
                &FsOpenOptions::new().write(true).append(true),
            )
            .unwrap();
        drop(f);
        assert_eq!(
            fs.state
                .lock()
                .durable
                .get(&real.join("old"))
                .map(Vec::as_slice),
            Some(&b"v1"[..]),
            "the unsynced write through the symlinked directory is not a baseline (tracking: {tracking})"
        );

        fs.crash();
        assert_eq!(read(&fs, real.join("new").to_str().unwrap()), b"data");
        assert_eq!(read(&fs, real.join("old").to_str().unwrap()), b"v1");
    }
}

/// A path through `..` names the file the backend resolves it to, in either
/// mode: an entry made as `sub/../d/f` is the entry `d/f`, made durable by a
/// sync of `d`, and a sync through that spelling is durable under every hard
/// link of the file.
#[cfg(unix)]
#[test]
fn a_dot_dot_spelling_names_the_file_it_resolves_to() {
    for tracking in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(dir.path()).unwrap();
        let d = base.join("d");
        let sub = base.join("sub");
        std::fs::create_dir(&d).unwrap();
        std::fs::create_dir(&sub).unwrap();
        let fs = CrashFs::new(crate::fs::StdFs);
        let fs = if tracking {
            fs.tracking_directory_entries()
        } else {
            fs
        };

        let mut f = fs
            .open(
                &sub.join("../d/f"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"v1").unwrap();
        f.sync_all().unwrap();
        drop(f);
        fs.hard_link(&d.join("f"), &d.join("link")).unwrap();
        fs.sync_directory(&d).unwrap();

        let mut f = fs
            .open(
                &sub.join("../d/f"),
                &FsOpenOptions::new().write(true).truncate(true),
            )
            .unwrap();
        f.write_all(b"v2").unwrap();
        f.sync_all().unwrap();
        drop(f);
        assert_eq!(
            fs.state
                .lock()
                .durable
                .get(&d.join("link"))
                .map(Vec::as_slice),
            Some(&b"v2"[..]),
            "a sync through another spelling is durable under the hard link (tracking: {tracking})"
        );

        fs.crash();
        assert_eq!(read(&fs, d.join("f").to_str().unwrap()), b"v2");
        assert_eq!(read(&fs, d.join("link").to_str().unwrap()), b"v2");
    }
}

/// A sync through a handle opened only for reading is as durable as any, in
/// either mode: opened through a symlink, it makes the file the link points
/// to durable, and removing the link afterwards keeps that.
#[cfg(unix)]
#[test]
fn a_read_only_sync_through_a_symlink_makes_its_target_durable() {
    for tracking in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let fs = CrashFs::new(crate::fs::StdFs);
        let fs = if tracking {
            fs.tracking_directory_entries()
        } else {
            fs
        };
        let target = dir.path().join("target");
        let link = dir.path().join("link");

        let mut f = fs
            .open(&target, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
        f.write_all(b"data").unwrap();
        drop(f);
        std::os::unix::fs::symlink("target", &link).unwrap();
        fs.sync_directory(dir.path()).unwrap();

        let f = fs.open(&link, &FsOpenOptions::new().read(true)).unwrap();
        f.sync_all().unwrap();
        drop(f);
        fs.remove_file(&link).unwrap();
        fs.sync_directory(dir.path()).unwrap();

        fs.crash();
        assert_eq!(
            read(&fs, target.to_str().unwrap()),
            b"data",
            "tracking: {tracking}"
        );
    }
}

/// A directory is the one the backend reaches, whatever the spelling: on a
/// filesystem that ignores case, a sync of `D` covers an entry made in `d`.
#[test]
fn a_sync_of_a_differently_cased_directory_covers_its_entries() {
    let dir = tempfile::tempdir().unwrap();
    let lower = dir.path().join("d");
    let upper = dir.path().join("D");
    std::fs::create_dir(&lower).unwrap();
    if !upper.exists() {
        // A case-sensitive filesystem: `D` is another directory.
        return;
    }
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();

    let mut f = fs
        .open(
            &lower.join("f"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(&upper).unwrap();

    fs.crash();
    assert_eq!(read(&fs, lower.join("f").to_str().unwrap()), b"data");
}

/// A rename that changes only the case of a name is a rename, even where the
/// filesystem still finds the old spelling: its new entry is lost on a crash
/// before its directory is synced, as any rename's is.
#[test]
fn a_rename_that_changes_only_the_case_is_a_rename() {
    let dir = tempfile::tempdir().unwrap();
    let probe = dir.path().join("probe");
    std::fs::write(&probe, b"").unwrap();
    if !dir.path().join("PROBE").exists() {
        // A case-sensitive filesystem: the two spellings are two files.
        return;
    }
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let upper = dir.path().join("Foo");
    let lower = dir.path().join("foo");
    let mut f = fs
        .open(&upper, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(dir.path()).unwrap();

    fs.rename(&upper, &lower).unwrap();
    assert!(
        fs.state
            .lock()
            .pending_entries
            .contains(&fs.name_of(&lower).unwrap()),
        "the new spelling is an entry its directory has not made durable"
    );
}

/// A rename from another case of an existing name to that name changes
/// nothing on a filesystem that ignores case: the durable file survives a
/// crash under its name.
#[test]
fn a_rename_from_another_case_onto_the_same_name_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let probe = dir.path().join("probe");
    std::fs::write(&probe, b"").unwrap();
    if !dir.path().join("PROBE").exists() {
        // A case-sensitive filesystem: the two spellings are two files.
        return;
    }
    let fs = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let name = dir.path().join("Foo");
    let mut f = fs
        .open(&name, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(dir.path()).unwrap();

    fs.rename(&dir.path().join("FOO"), &name).unwrap();

    fs.crash();
    assert_eq!(read(&fs, name.to_str().unwrap()), b"data");
}

/// A backend that does not resolve `..` keeps a name through it as a name of
/// its own: `MemFs` holds `/d/../a` apart from `/a`, so the simulator tracks
/// it under that spelling and a crash makes no file at `/a`.
#[test]
fn a_dot_dot_name_stays_literal_on_a_backend_that_keeps_it() {
    let fs = CrashFs::new(MemFs::new()).tracking_directory_entries();
    fs.create_dir_all(Path::new("/d/..")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/../a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.sync_directory(Path::new("/d/..")).unwrap();

    fs.crash();
    assert_eq!(read(&fs, "/d/../a"), b"data");
    assert!(!fs.exists(Path::new("/a")).unwrap());
}

/// A fault layer composes above the simulator, so the probes the simulator
/// makes for its own bookkeeping see the disk: a fault that makes the source
/// look absent to the caller does not turn a rename between two links of one
/// file into a move, and both durable names survive a crash.
#[cfg(unix)]
#[test]
fn a_fault_above_the_simulator_leaves_a_no_op_rename_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let crash = CrashFs::new(crate::fs::StdFs).tracking_directory_entries();
    let fs = FaultFs::new(crash.clone());
    let injector = fs.injector();
    let src = dir.path().join("src");
    let link = dir.path().join("link");

    let mut f = fs
        .open(&src, &FsOpenOptions::new().write(true).create(true))
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    drop(f);
    fs.hard_link(&src, &link).unwrap();
    fs.sync_directory(dir.path()).unwrap();
    injector.arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(link.display().to_string()),
    );
    fs.rename(&link, &src).unwrap();
    injector.clear();

    crash.crash();
    assert_eq!(read(&crash, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&crash, link.to_str().unwrap()), b"durable");
}

/// The blob files a flush and an ingestion write survive a power loss once
/// the write returns: the manifest that names them must not outlive them.
#[test]
fn blob_files_of_an_acknowledged_write_survive_a_crash() -> crate::Result<()> {
    use crate::{AbstractTree, KvSeparationOptions, SequenceNumberCounter};

    let crash = CrashFs::new(MemFs::new()).tracking_directory_entries();
    let open = |fs: Arc<dyn Fs>| {
        crate::Config::new(
            "/db",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .with_shared_fs(fs)
        .open()
    };

    {
        let tree = open(Arc::new(crash.clone()))?;
        tree.insert("flushed", "blob value of a flush", 0);
        tree.flush_active_memtable(0)?;
        let mut ingestion = tree.ingestion()?;
        ingestion.write("ingested", "blob value of an ingestion")?;
        ingestion.finish()?;
    }

    crash.crash();

    let tree = open(crash.inner())?;
    assert_eq!(
        tree.get("flushed", u64::MAX)?.as_deref(),
        Some(&b"blob value of a flush"[..]),
    );
    assert_eq!(
        tree.get("ingested", u64::MAX)?.as_deref(),
        Some(&b"blob value of an ingestion"[..]),
    );
    Ok(())
}

/// The manifest's edit log, created by the first flush after a snapshot,
/// survives a power loss with the flushes it recorded.
#[test]
fn the_edit_log_of_acknowledged_flushes_survives_a_crash() -> crate::Result<()> {
    use crate::{AbstractTree, SequenceNumberCounter};

    let crash = CrashFs::new(MemFs::new()).tracking_directory_entries();
    let open = |fs: Arc<dyn Fs>| {
        crate::Config::new(
            "/db",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .open()
    };

    {
        let tree = open(Arc::new(crash.clone()))?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(0)?;
    }

    crash.crash();

    let tree = open(crash.inner())?;
    assert!(tree.contains_key("a", u64::MAX)?);
    assert!(tree.contains_key("b", u64::MAX)?);
    Ok(())
}

/// A failed sync of the new edit log's directory fails the flush, and the
/// next flush syncs it again: the log is no longer empty then, but its
/// directory entry is still not durable, and a flush acknowledged over it
/// would be lost with it.
#[test]
fn a_failed_sync_of_the_edit_log_directory_is_retried() -> crate::Result<()> {
    use crate::{AbstractTree, SequenceNumberCounter};

    let crash = CrashFs::new(MemFs::new()).tracking_directory_entries();
    let fault = FaultFs::new(crash.clone());
    let injector = fault.injector();
    let open = |fs: Arc<dyn Fs>| {
        crate::Config::new(
            "/db",
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .open()
    };

    {
        let tree = open(Arc::new(fault))?;
        // A flush syncs its table's directory, then, on the first edit of a
        // generation, the log's: fail the second.
        injector.arm(
            FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
                .skip(1)
                .once(),
        );
        tree.insert("a", "1", 0);
        match tree.flush_active_memtable(0) {
            Ok(()) => panic!("the injected directory sync fault must fail the flush"),
            Err(e) => assert!(format!("{e}").contains("injected fault"), "{e}"),
        }
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(0)?;
    }

    crash.crash();

    let tree = open(crash.inner())?;
    assert!(
        tree.contains_key("b", u64::MAX)?,
        "the acknowledged flush survives the crash"
    );
    Ok(())
}

#[test]
fn synced_content_survives_crash() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    drop(f);

    fs.crash();
    assert_eq!(read(&fs, "/d/a"), b"durable");
}

#[test]
fn unsynced_tail_is_rolled_back() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"durable").unwrap();
    f.sync_all().unwrap();
    // Append more, but never sync: this tail must vanish on crash.
    f.write_all(b"+volatile").unwrap();
    drop(f);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/a"),
        b"durable",
        "only the bytes durable at the last fsync survive"
    );
}

#[test]
fn never_synced_file_vanishes_on_crash() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/ghost"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"never synced").unwrap();
    drop(f);

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/ghost")).unwrap(),
        "a file written but never fsynced does not survive a crash"
    );
}

#[test]
fn re_sync_advances_the_durable_image() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let opts = FsOpenOptions::new().write(true).create(true).truncate(true);

    let mut f = fs.open(Path::new("/d/a"), &opts).unwrap();
    f.write_all(b"v1").unwrap();
    f.sync_all().unwrap();
    drop(f);

    let mut f = fs.open(Path::new("/d/a"), &opts).unwrap();
    f.write_all(b"v2-longer").unwrap();
    f.sync_all().unwrap();
    drop(f);

    let mut f = fs.open(Path::new("/d/a"), &opts).unwrap();
    f.write_all(b"v3-unsynced").unwrap();
    drop(f);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/a"),
        b"v2-longer",
        "crash rolls back to the most recent synced image, not the first"
    );
}

#[test]
fn unsynced_truncate_is_rolled_back() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"12345").unwrap();
    f.sync_all().unwrap();
    // Shrink without syncing: the truncate must not be durable.
    f.set_len(2).unwrap();
    drop(f);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/a"),
        b"12345",
        "an un-synced truncate is undone, restoring the synced length"
    );
}

#[test]
fn rename_carries_the_durable_image() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);

    fs.rename(Path::new("/d/src"), Path::new("/d/dst")).unwrap();

    // Append to the renamed file without syncing.
    let mut f = fs
        .open(
            Path::new("/d/dst"),
            &FsOpenOptions::new().write(true).append(true),
        )
        .unwrap();
    f.write_all(b"+more").unwrap();
    drop(f);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/dst"),
        b"data",
        "the durable image follows the rename; the un-synced append is undone"
    );
    assert!(
        !fs.exists(Path::new("/d/src")).unwrap(),
        "the renamed-away source does not reappear after a crash"
    );
}

#[test]
fn delegates_the_full_surface_transparently() {
    // With no crash invoked, CrashFs must be a faithful pass-through to its
    // inner backend across the whole Fs / FsFile surface. Exercises every
    // delegating method so a regression in any forward is caught.
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    fs.create_dir(Path::new("/d/sub")).unwrap();

    // Open + the full FsFile surface.
    let mut f = fs
        .open(
            Path::new("/d/f"),
            &FsOpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap();
    f.write_all(b"hello world").unwrap();
    f.flush().unwrap();
    f.sync_data().unwrap();
    f.sync_all().unwrap();
    f.sync_data_with(SyncMode::Normal).unwrap();
    f.sync_all_with(SyncMode::Full).unwrap();
    assert_eq!(f.seek(SeekFrom::Start(0)).unwrap(), 0);
    let mut buf = [0u8; 5];
    assert_eq!(f.read_at(&mut buf, 0).unwrap(), 5);
    assert_eq!(&buf, b"hello");
    assert_eq!(FsFile::metadata(&*f).unwrap().len, 11);
    f.set_len(11).unwrap();
    f.hint(FileHint::Sequential).unwrap();
    // MemFs is single-process: the lock is vacuous but must still delegate.
    assert!(f.try_lock_exclusive().unwrap());
    f.lock_exclusive().unwrap();
    drop(f);

    // Fs-level metadata / existence / listing.
    assert_eq!(fs.metadata(Path::new("/d/f")).unwrap().len, 11);
    assert!(fs.exists(Path::new("/d/f")).unwrap());
    assert!(!fs.exists(Path::new("/d/missing")).unwrap());
    assert!(!fs.read_dir(Path::new("/d")).unwrap().is_empty());

    // Directory + whole-file sync.
    fs.sync_directory(Path::new("/d")).unwrap();
    fs.sync_directory_with(Path::new("/d"), SyncMode::Full)
        .unwrap();

    // Identity / capability probes forward to the inner backend.
    assert!(fs.backend_id().is_some());
    // `inner()` hands back the wrapped backend (used to reopen after a crash).
    assert_eq!(fs.inner().backend_id(), fs.backend_id());
    let _ = fs.volume_id(Path::new("/d"));
    assert!(fs.capabilities(Path::new("/d")).punch_hole);
    assert_eq!(fs.available_space(Path::new("/d")).unwrap(), u64::MAX);

    // Copy-style operations (MemFs implements link/reflink as byte copies).
    fs.hard_link(Path::new("/d/f"), Path::new("/d/link"))
        .unwrap();
    assert_eq!(read(&fs, "/d/link"), b"hello world");
    fs.reflink_file(Path::new("/d/f"), Path::new("/d/clone"))
        .unwrap();
    assert_eq!(read(&fs, "/d/clone"), b"hello world");

    // Best-effort capability hooks: MemFs leaves the defaults (no-op / unsupported).
    fs.try_disable_cow(Path::new("/d/f")).unwrap();
    fs.punch_hole(Path::new("/d/f"), 0, 4).unwrap();
    let _ = fs.hard_link_count(Path::new("/d/f"));

    // Truncate-to-zero reclaim + removal.
    fs.truncate_file(Path::new("/d/clone")).unwrap();
    assert_eq!(fs.metadata(Path::new("/d/clone")).unwrap().len, 0);
    fs.remove_file(Path::new("/d/link")).unwrap();
    assert!(!fs.exists(Path::new("/d/link")).unwrap());
}

#[test]
fn pre_existing_contents_are_treated_as_durable() {
    // A file that already exists when the wrapper is created is "already
    // durable" per the contract: opening it for an unsynced write and crashing
    // must roll it back to its original bytes, not remove it.
    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    {
        let mut f = mem
            .open(
                Path::new("/d/pre"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"original").unwrap();
    }
    let fs = CrashFs::new(mem);

    let mut f = fs
        .open(
            Path::new("/d/pre"),
            &FsOpenOptions::new().write(true).append(true),
        )
        .unwrap();
    f.write_all(b"+unsynced").unwrap();
    drop(f);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/pre"),
        b"original",
        "a pre-existing file rolls back to its initial contents, not removed"
    );
}

#[test]
fn rename_over_synced_destination_does_not_resurrect_it() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    // Synced destination holding "old".
    let mut dst = fs
        .open(
            Path::new("/d/dst"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    dst.write_all(b"old").unwrap();
    dst.sync_all().unwrap();
    drop(dst);

    // Unsynced source, then replace the destination with it.
    let mut src = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    src.write_all(b"new").unwrap();
    drop(src);
    fs.rename(Path::new("/d/src"), Path::new("/d/dst")).unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/dst")).unwrap(),
        "an unsynced rename over a synced destination does not crash back to the stale destination"
    );
}

#[test]
fn remove_dir_all_clears_crash_state_under_the_directory() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut f = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"A").unwrap();
    f.sync_all().unwrap();
    drop(f);

    fs.remove_dir_all(Path::new("/d")).unwrap();

    // crash() must not resurrect (or panic recreating) a file whose directory
    // was removed.
    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/a")).unwrap(),
        "a file under a removed directory is not resurrected by crash()"
    );
}

#[test]
fn hard_linked_durable_file_rolls_back_not_removed() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut src = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    src.write_all(b"data").unwrap();
    src.sync_all().unwrap();
    drop(src);

    fs.hard_link(Path::new("/d/src"), Path::new("/d/link"))
        .unwrap();

    // Open the durable copy for an unsynced write, then crash.
    let mut l = fs
        .open(
            Path::new("/d/link"),
            &FsOpenOptions::new().write(true).append(true),
        )
        .unwrap();
    l.write_all(b"+x").unwrap();
    drop(l);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/link"),
        b"data",
        "a hard-linked durable copy rolls back to the linked bytes, not removed"
    );
}

#[test]
fn reflinked_durable_file_rolls_back_not_removed() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    let mut src = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    src.write_all(b"data").unwrap();
    src.sync_all().unwrap();
    drop(src);

    fs.reflink_file(Path::new("/d/src"), Path::new("/d/clone"))
        .unwrap();

    let mut c = fs
        .open(
            Path::new("/d/clone"),
            &FsOpenOptions::new().write(true).append(true),
        )
        .unwrap();
    c.write_all(b"+x").unwrap();
    drop(c);

    fs.crash();
    assert_eq!(
        read(&fs, "/d/clone"),
        b"data",
        "a reflinked durable copy rolls back to the cloned bytes, not removed"
    );
}

#[test]
fn hard_linked_pre_existing_untouched_source_survives_crash() {
    // A hard link whose source already existed when the wrapper was created
    // (durable on disk, but never touched through CrashFs this run) must survive
    // a crash carrying the source's bytes. The copy was made from durable
    // content, so crash() must restore it, not remove it as if un-synced.
    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    {
        let mut f = mem
            .open(
                Path::new("/d/src"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"durable").unwrap();
    }
    let fs = CrashFs::new(mem);

    // Source is pre-existing and never touched through the wrapper, so it has
    // no captured durable image yet; the link must still inherit its baseline.
    fs.hard_link(Path::new("/d/src"), Path::new("/d/link"))
        .unwrap();

    fs.crash();
    assert!(
        fs.exists(Path::new("/d/link")).unwrap(),
        "a hard link from a pre-existing durable source must not be removed on crash"
    );
    assert_eq!(
        read(&fs, "/d/link"),
        b"durable",
        "the hard link rolls back to the source's durable bytes"
    );
}

#[test]
fn reflinked_pre_existing_untouched_source_survives_crash() {
    // Reflink counterpart: a clone of a pre-existing durable source (never
    // touched through the wrapper) must survive a crash with the source's bytes.
    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    {
        let mut f = mem
            .open(
                Path::new("/d/src"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"durable").unwrap();
    }
    let fs = CrashFs::new(mem);

    fs.reflink_file(Path::new("/d/src"), Path::new("/d/clone"))
        .unwrap();

    fs.crash();
    assert!(
        fs.exists(Path::new("/d/clone")).unwrap(),
        "a reflink from a pre-existing durable source must not be removed on crash"
    );
    assert_eq!(
        read(&fs, "/d/clone"),
        b"durable",
        "the reflink rolls back to the source's durable bytes"
    );
}

// The `# Panics` contract: crash() surfaces an inner-backend failure loudly
// rather than silently under-testing recovery. Driven by wrapping a FaultFs as
// the inner backend and failing the operation crash() performs.

#[test]
#[should_panic(expected = "removing un-synced")]
fn crash_panics_when_removing_an_unsynced_file_fails() {
    let fault = FaultFs::new(MemFs::new());
    let inj = fault.injector();
    let fs = CrashFs::from_shared(Arc::new(fault));
    fs.create_dir_all(Path::new("/d")).unwrap();
    // Unsynced file: touched, no durable image -> crash() takes the remove path.
    let mut f = fs
        .open(
            Path::new("/d/ghost"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"x").unwrap();
    drop(f);
    // Make the inner remove fail with a non-NotFound error.
    inj.arm(FaultRule::new(
        FaultOp::RemoveFile,
        Fault::Error(ErrorKind::Other),
    ));
    fs.crash();
}

#[test]
#[should_panic(expected = "reopening")]
fn crash_panics_when_restore_open_fails() {
    let fault = FaultFs::new(MemFs::new());
    let inj = fault.injector();
    let fs = CrashFs::from_shared(Arc::new(fault));
    fs.create_dir_all(Path::new("/d")).unwrap();
    let mut f = fs
        .open(
            Path::new("/d/f"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap(); // durable image recorded
    drop(f);
    // The rollback reopen now fails.
    inj.arm(FaultRule::new(
        FaultOp::Open,
        Fault::Error(ErrorKind::Other),
    ));
    fs.crash();
}

#[test]
#[should_panic(expected = "rewriting durable image")]
fn crash_panics_when_restore_write_fails() {
    let fault = FaultFs::new(MemFs::new());
    let inj = fault.injector();
    let fs = CrashFs::from_shared(Arc::new(fault));
    fs.create_dir_all(Path::new("/d")).unwrap();
    let mut f = fs
        .open(
            Path::new("/d/f"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"data").unwrap();
    f.sync_all().unwrap();
    drop(f);
    // Rollback reopen succeeds (Open not armed), but the rewrite fails.
    inj.arm(FaultRule::new(
        FaultOp::Write,
        Fault::Error(ErrorKind::Other),
    ));
    fs.crash();
}

#[test]
fn baseline_read_failure_surfaces_from_open() {
    // A failed baseline read must NOT be swallowed into "no durable image" (which
    // would later remove a pre-existing file on crash); it must surface from open().
    let fault = FaultFs::new(MemFs::new());
    let inj = fault.injector();
    fault.create_dir_all(Path::new("/d")).unwrap();
    {
        let mut f = fault
            .open(
                Path::new("/d/pre"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        std::io::Write::write_all(&mut f, b"original").unwrap();
    }
    let fs = CrashFs::from_shared(Arc::new(fault));

    // Fail the baseline read (FsFile::read) on the first write-open of the file.
    inj.arm(FaultRule::new(
        FaultOp::Read,
        Fault::Error(ErrorKind::Other),
    ));
    assert!(
        fs.open(
            Path::new("/d/pre"),
            &FsOpenOptions::new().write(true).append(true),
        )
        .is_err(),
        "a failed baseline read surfaces from open(), it is not silently dropped"
    );
}

/// A hard link the backend made is a new entry even when reading its source's
/// baseline then fails: its directory was never synced, so a crash removes it
/// rather than keeping an entry the simulator never recorded.
#[test]
fn a_hard_link_whose_baseline_read_fails_does_not_survive_a_crash() {
    let fault = FaultFs::new(MemFs::new());
    let inj = fault.injector();
    fault.create_dir_all(Path::new("/d")).unwrap();
    {
        let mut f = fault
            .open(
                Path::new("/d/pre"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        std::io::Write::write_all(&mut f, b"original").unwrap();
    }
    let fs = CrashFs::from_shared(Arc::new(fault)).tracking_directory_entries();

    // The link is made; reading the untouched source's baseline then fails.
    inj.arm(FaultRule::new(
        FaultOp::Read,
        Fault::Error(ErrorKind::Other),
    ));
    assert!(
        fs.hard_link(Path::new("/d/pre"), Path::new("/d/link"))
            .is_err()
    );
    assert!(
        fs.exists(Path::new("/d/link")).unwrap(),
        "the backend made it"
    );

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/link")).unwrap(),
        "an entry whose directory was never synced does not survive a crash"
    );
}

#[test]
fn hard_link_of_unsynced_source_does_not_survive_crash() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    // Never-synced source.
    let mut f = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);
    // Link it without ever syncing or opening the destination.
    fs.hard_link(Path::new("/d/src"), Path::new("/d/link"))
        .unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/link")).unwrap(),
        "a link to a never-synced source carries no durable image and is removed on crash"
    );
}

#[test]
fn reflink_of_unsynced_source_does_not_survive_crash() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    let mut f = fs
        .open(
            Path::new("/d/src"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);
    fs.reflink_file(Path::new("/d/src"), Path::new("/d/clone"))
        .unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/clone")).unwrap(),
        "a reflink of a never-synced source carries no durable image and is removed on crash"
    );
}

#[test]
fn reopening_an_unsynced_file_does_not_promote_it_to_durable() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    let mut f = fs
        .open(
            Path::new("/d/f"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    f.write_all(b"unsynced").unwrap();
    drop(f);
    // Re-open for writing WITHOUT syncing: the un-synced bytes must not be
    // captured as a durable baseline.
    drop(
        fs.open(Path::new("/d/f"), &FsOpenOptions::new().write(true))
            .unwrap(),
    );

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/f")).unwrap(),
        "re-opening a never-synced file does not promote its un-synced bytes to durable"
    );
}

#[test]
fn independent_files_have_independent_durability() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();

    // a: synced. b: not synced.
    let mut a = fs
        .open(
            Path::new("/d/a"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    a.write_all(b"A").unwrap();
    a.sync_all().unwrap();
    drop(a);

    let mut b = fs
        .open(
            Path::new("/d/b"),
            &FsOpenOptions::new().write(true).create(true),
        )
        .unwrap();
    b.write_all(b"B").unwrap();
    drop(b);

    fs.crash();
    assert_eq!(read(&fs, "/d/a"), b"A", "synced file survives");
    assert!(
        !fs.exists(Path::new("/d/b")).unwrap(),
        "un-synced sibling vanishes independently"
    );
}
