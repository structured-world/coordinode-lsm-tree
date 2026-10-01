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
    fs.sync_directory(Path::new("/d")).unwrap();

    fs.crash();
    assert_eq!(read(&fs, "/d/a"), b"durable");
}

/// A new file's directory entry is durable only once its directory is synced:
/// a file whose content was synced but whose directory never was is lost,
/// and a sync of another directory does not save it.
#[test]
fn a_synced_file_in_an_unsynced_directory_is_lost() {
    let fs = CrashFs::new(MemFs::new());
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

/// A rename's new name is an entry of its own: without a sync of its
/// directory it is lost, and the old name was removed.
#[test]
fn a_rename_without_a_directory_sync_is_lost() {
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
    fs.sync_directory(Path::new("/d")).unwrap();
    fs.rename(Path::new("/d/src"), Path::new("/d/dst")).unwrap();

    fs.crash();
    assert!(
        !fs.exists(Path::new("/d/dst")).unwrap(),
        "a renamed-to name whose directory was not synced does not survive a crash"
    );
}

/// Every operation that makes or removes a directory entry, and a directory
/// sync, runs with the namespace held, so none lands inside another: not a
/// creation between a sync's backend call and what the sync is credited
/// with, not a removal between an open's existence probe and the open, not a
/// new entry at a renamed-away path before the rename checks it. Each waits
/// while another holds the namespace.
#[test]
fn namespace_operations_wait_for_one_in_flight() {
    let fs = CrashFs::new(MemFs::new());
    fs.create_dir_all(Path::new("/d")).unwrap();
    for name in ["/d/gone", "/d/src", "/d/linked"] {
        let mut f = fs
            .open(
                Path::new(name),
                &FsOpenOptions::new().write(true).create(true),
            )
            .unwrap();
        f.write_all(b"x").unwrap();
    }

    type Operation = (&'static str, fn(&CrashFs));
    let operations: [Operation; 6] = [
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

/// Whether a rename between two names is a no-op is known from the files the
/// names refer to before it runs, not from probing the source afterwards: a
/// probe that wrongly reports the source gone does not turn the no-op into a
/// move that leaves the destination's durable name unsynced.
#[cfg(unix)]
#[test]
fn a_no_op_rename_is_known_before_it_runs() {
    let dir = tempfile::tempdir().unwrap();
    let faulty = FaultFs::new(crate::fs::StdFs);
    let injector = faulty.injector();
    let fs = CrashFs::new(faulty);
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
    // An existence probe of the source reports it absent.
    injector.arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(ErrorKind::Other))
            .on_path(link.display().to_string()),
    );
    fs.rename(&link, &src).unwrap();
    injector.clear();

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&fs, link.to_str().unwrap()), b"durable");
}

/// A rename whose names cannot be told apart fails before it runs: the probe's
/// error is returned, nothing is renamed, and both durable names survive.
#[cfg(unix)]
#[test]
fn a_rename_whose_identity_probe_fails_moves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let faulty = FaultFs::new(crate::fs::StdFs);
    let injector = faulty.injector();
    let fs = CrashFs::new(faulty);
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
        FaultRule::new(FaultOp::SameFile, Fault::Error(ErrorKind::Other))
            .on_path(link.display().to_string()),
    );
    let error = fs.rename(&link, &src).unwrap_err();
    injector.clear();
    assert_eq!(error.kind(), ErrorKind::Other);

    fs.crash();
    assert_eq!(read(&fs, src.to_str().unwrap()), b"durable");
    assert_eq!(read(&fs, link.to_str().unwrap()), b"durable");
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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();

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
    fs.sync_directory(Path::new("/d")).unwrap();
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
    fs.sync_directory(Path::new("/d")).unwrap();
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
    let fs = CrashFs::from_shared(Arc::new(fault));

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
    fs.sync_directory(Path::new("/d")).unwrap();

    fs.crash();
    assert_eq!(read(&fs, "/d/a"), b"A", "synced file survives");
    assert!(
        !fs.exists(Path::new("/d/b")).unwrap(),
        "un-synced sibling vanishes independently"
    );
}
