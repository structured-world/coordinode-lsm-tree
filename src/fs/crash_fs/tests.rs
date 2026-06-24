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
