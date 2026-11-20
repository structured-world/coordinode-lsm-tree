use super::sync_on_ring;
use crate::fs::{Fs, StdFs, SyncMode, SyncTarget, WriteOrigin};
use std::io::Write;

/// A ring that answers the sync opcode as unknown (`EINVAL`, `EOPNOTSUPP`:
/// a partial `io_uring`) refuses the batch, for it to go another way; any
/// other failure is the sync's own. Guards against every batch failing on a
/// runtime that sets rings up and syncs nothing through them.
#[test]
fn only_an_unknown_opcode_refuses_the_ring() {
    assert!(super::refuses_sync(libc::EINVAL));
    assert!(super::refuses_sync(libc::EOPNOTSUPP));
    assert!(!super::refuses_sync(libc::EIO));
    assert!(!super::refuses_sync(libc::ENOSPC));
}

/// A wait that fails before every sync is reaped spends each sync reaped as
/// failed, each reaped as refused, and each not reaped. Guards against a
/// target whose sync answered `EINVAL` in an earlier, interrupted wait being
/// left out of the failure, and a later persist naming its file.
#[test]
fn a_batch_cut_short_spends_its_refused_syncs_too() {
    use super::Completions;

    let mut completions = Completions::default();
    completions.note(0, 0);
    completions.note(1, -libc::EINVAL);
    completions.note(2, -libc::EIO);
    let failure =
        completions.cut_short(std::io::Error::other("the wait failed"), [3, 4].into_iter());
    let mut spent = failure.spent;
    spent.sort_unstable();
    assert_eq!(spent, [1, 2, 3, 4], "every sync not known synced is spent");
}

/// A wait that fails after the kernel took part of the queue spends only the
/// syncs it took and did not complete; those still queued never ran and stay
/// to be synced again. Guards against a target whose sync never reached the
/// kernel being marked failed, and every later persist naming its file
/// failing before it is retried.
#[test]
fn a_batch_cut_short_leaves_what_the_kernel_never_took() {
    // Four pushed: the first reaped, the second taken and not reaped, the
    // last two still queued.
    let spent: Vec<usize> = super::submitted_unreaped(&[true, false, false, false], 2).collect();
    assert_eq!(spent, [1], "only the sync the kernel took is spent");
    let all: Vec<usize> = super::submitted_unreaped(&[true, false, false], 0).collect();
    assert_eq!(
        all,
        [1, 2],
        "with nothing queued, every unreaped sync is spent"
    );
}

/// Only a batch whose every sync answered as an unknown opcode would is
/// refused. Beside a sync that ran, in either order and whether it failed or
/// not, that answer is its target's own (a `/dev/null` has no sync): the batch
/// fails with each failed target spent, and the ring stays on. Guards against
/// one target that cannot be synced turning the ring off for the thread and
/// its sync being retried through a new handle.
#[test]
fn only_a_batch_every_sync_refused_is_refused() {
    use super::{Completions, NO_RING_SYNC, Through};

    NO_RING_SYNC.set(false);
    for results in [[-libc::EIO, -libc::EINVAL], [-libc::EINVAL, -libc::EIO]] {
        let mut completions = Completions::default();
        for (at, result) in results.into_iter().enumerate() {
            completions.note(at, result);
        }
        let Err(e) = completions.outcome(false) else {
            panic!("the failed syncs are reported: {results:?}");
        };
        let mut spent = e.spent;
        spent.sort_unstable();
        assert_eq!(spent, [0, 1], "both syncs failed: {results:?}");
    }
    for results in [[0, -libc::EINVAL], [-libc::EOPNOTSUPP, 0]] {
        let mut completions = Completions::default();
        for (at, result) in results.into_iter().enumerate() {
            completions.note(at, result);
        }
        let Err(e) = completions.outcome(false) else {
            panic!("a target that has no sync fails the batch: {results:?}");
        };
        let refused_at = results
            .iter()
            .position(|&result| result != 0)
            .unwrap_or(usize::MAX);
        assert_eq!(e.spent, [refused_at], "{results:?}");
    }
    assert!(!NO_RING_SYNC.get(), "the ring stays on");

    let refused = || {
        let mut refused = Completions::default();
        for (at, result) in [-libc::EINVAL, -libc::EOPNOTSUPP].into_iter().enumerate() {
            refused.note(at, result);
        }
        refused
    };
    assert!(matches!(refused().outcome(false), Ok(Through::Refused)));
    // A ring a sync completed through before runs the opcode: the same
    // answers are the targets' own.
    let Err(e) = refused().outcome(true) else {
        panic!("on a proven ring the refused syncs fail the batch");
    };
    let mut spent = e.spent;
    spent.sort_unstable();
    assert_eq!(spent, [0, 1]);

    let mut synced = Completions::default();
    synced.note(0, 0);
    assert!(matches!(synced.outcome(false), Ok(Through::Synced)));
}

/// A chunk whose every sync the ring refuses, after a chunk of the same batch
/// synced through it, fails the batch at that target's own index and leaves
/// the ring on: the first chunk proved the ring runs the opcode. Guards
/// against one target with no sync, landing alone in a later chunk, turning
/// the ring off for the thread and sending the whole batch through threads.
#[test]
fn a_later_chunk_refused_on_a_proven_ring_fails_at_its_target() -> crate::Result<()> {
    use super::{MAX_IN_FLIGHT, NO_RING_SYNC};

    NO_RING_SYNC.set(false);
    let dir = tempfile::tempdir()?;
    let paths: Vec<std::path::PathBuf> = (0..MAX_IN_FLIGHT)
        .map(|i| dir.path().join(format!("f{i}")))
        .collect();
    for path in &paths {
        std::fs::File::create(path)?.write_all(b"x")?;
    }
    let mut targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    targets.push(SyncTarget::FileData {
        path: std::path::Path::new("/dev/null"),
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    });
    // A kernel that refuses io_uring has nothing to test here.
    let Some(result) = sync_on_ring(&targets) else {
        assert!(
            sync_on_ring(targets.get(..1).unwrap_or_default()).is_none(),
            "the batch went another way only for want of a ring"
        );
        return Ok(());
    };
    let Err(failure) = result else {
        panic!("the target with no sync fails the batch");
    };
    assert_eq!(failure.spent, [MAX_IN_FLIGHT], "at its own index");
    assert!(!NO_RING_SYNC.get(), "the ring stays on");
    Ok(())
}

/// Once a ring refused the opcode on a thread, its batches go another way
/// without setting a ring up again. Guards against every batch paying a ring
/// that syncs nothing.
#[test]
fn a_thread_whose_ring_refused_the_opcode_asks_no_ring_again() -> crate::Result<()> {
    use super::NO_RING_SYNC;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("a");
    std::fs::File::create(&path)?;
    let targets = [SyncTarget::FileData {
        path: &path,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    }];
    NO_RING_SYNC.set(true);
    let through_ring = sync_on_ring(&targets);
    NO_RING_SYNC.set(false);
    assert!(through_ring.is_none(), "the batch goes another way");
    Ok(())
}

/// A sync the queue has no room for fails the batch, and no sync queued
/// before it is spent: nothing was submitted, and a ring without a polling
/// kernel thread runs only what is submitted. Guards against those targets
/// being marked as failed syncs, which fails every later persist naming them
/// though none of their syncs ran.
#[test]
fn a_full_submission_queue_fails_the_batch_and_spends_nothing() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let paths = [dir.path().join("a"), dir.path().join("b")];
    for path in &paths {
        std::fs::File::create(path)?;
    }
    let targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    // One entry for two syncs.
    let Ok(mut ring) = io_uring::IoUring::new(1) else {
        return Ok(());
    };
    let Err(failure) = super::sync_through(&mut ring, 5, &targets) else {
        panic!("two syncs fit a queue of one");
    };
    assert!(
        failure.spent.is_empty(),
        "the queued sync never ran: {:?}",
        failure.spent
    );
    Ok(())
}

/// A submission the kernel took none of turns the ring off for the thread
/// only when its error says the ring is refused for good; a shortage that may
/// pass sends this batch another way and leaves the ring on. Guards against
/// one `EAGAIN` under memory pressure costing the thread its ring for good.
#[test]
fn only_a_lasting_refusal_of_a_submission_turns_the_ring_off() {
    use super::Through;

    for errno in [libc::EPERM, libc::ENOSYS, libc::EOPNOTSUPP, libc::EINVAL] {
        assert_eq!(super::unsubmitted(Some(errno)), Through::Refused, "{errno}");
    }
    for errno in [
        Some(libc::EAGAIN),
        Some(libc::ENOMEM),
        Some(libc::EBUSY),
        None,
    ] {
        assert_eq!(super::unsubmitted(errno), Through::Unsubmitted, "{errno:?}");
    }
}

/// A batch of file and folder syncs goes through one ring and reports every
/// one done: the path a synced flush takes on Linux.
#[test]
fn a_batch_of_files_and_folders_syncs_through_one_ring() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut paths = Vec::new();
    for name in ["a", "b", "c"] {
        let path = dir.path().join(name);
        std::fs::File::create(&path)?.write_all(name.as_bytes())?;
        paths.push(path);
    }
    let mut targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    targets.push(SyncTarget::Folder {
        path: dir.path(),
        mode: SyncMode::Normal,
    });
    let Some(result) = sync_on_ring(&targets) else {
        // A kernel that refuses io_uring has nothing to test here; the batch
        // is synced on threads instead (see the next test).
        return Ok(());
    };
    result?;
    // The same batch through the backend, whichever way it syncs.
    StdFs.sync_many(&targets)?;
    Ok(())
}

/// Files kept open since their writes sync through the ring by the
/// descriptors their std handles hold, the batch a synced flush makes on
/// Linux. Guards against those batches leaving the ring for threads, and
/// turning it off for the thread.
#[test]
fn files_kept_open_sync_through_the_ring() -> crate::Result<()> {
    use super::NO_RING_SYNC;
    use crate::fs::FsOpenOptions;

    NO_RING_SYNC.set(false);
    let dir = tempfile::tempdir()?;
    let paths = [dir.path().join("a"), dir.path().join("b")];
    let mut files = Vec::new();
    for path in &paths {
        let mut file = StdFs.open(path, &FsOpenOptions::new().write(true).create_new(true))?;
        file.write_all(b"x")?;
        files.push(file);
    }
    let targets: Vec<SyncTarget<'_>> = files
        .iter()
        .zip(&paths)
        .map(|(file, path)| SyncTarget::OpenFile {
            file: &**file,
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    let regular = SyncTarget::FileData {
        path: &paths[0],
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    };
    // A kernel that refuses io_uring has nothing to test here.
    if sync_on_ring(core::slice::from_ref(&regular)).is_none() {
        return Ok(());
    }
    assert!(
        matches!(sync_on_ring(&targets), Some(Ok(()))),
        "the kept files sync through the ring"
    );
    assert!(!NO_RING_SYNC.get(), "the ring stays on");
    Ok(())
}

/// A handle kept open with no descriptor behind it sends its batch another
/// way, and only that batch: the next batch of files with descriptors still
/// syncs through the ring. Guards against one such handle turning the ring
/// off for every later batch of the thread.
#[test]
fn a_kept_handle_without_a_descriptor_leaves_the_ring_on() -> crate::Result<()> {
    use super::NO_RING_SYNC;
    use crate::fs::{FsOpenOptions, MemFs};

    NO_RING_SYNC.set(false);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("a");
    std::fs::File::create(&path)?.write_all(b"x")?;
    let regular = SyncTarget::FileData {
        path: &path,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    };
    if sync_on_ring(core::slice::from_ref(&regular)).is_none() {
        return Ok(());
    }
    let memory = MemFs::new();
    let in_memory = std::path::Path::new("/m");
    let file = memory.open(
        in_memory,
        &FsOpenOptions::new().write(true).create_new(true),
    )?;
    let descriptorless = SyncTarget::OpenFile {
        file: &*file,
        path: in_memory,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    };
    assert!(
        sync_on_ring(&[regular, descriptorless]).is_none(),
        "the batch goes another way"
    );
    assert!(!NO_RING_SYNC.get(), "the ring stays on for the thread");
    assert!(
        matches!(sync_on_ring(core::slice::from_ref(&regular)), Some(Ok(()))),
        "the next batch syncs through the ring"
    );
    Ok(())
}

/// A target that cannot be opened fails the batch with its own error, before
/// anything is submitted: a missing file is reported missing, not synced.
#[test]
fn a_missing_file_fails_the_batch_with_not_found() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let present = dir.path().join("present");
    std::fs::File::create(&present)?;
    let missing = dir.path().join("missing");
    let targets = [
        SyncTarget::FileData {
            path: &present,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        },
        SyncTarget::FileData {
            path: &missing,
            origin: WriteOrigin::FlushBlob,
            mode: SyncMode::Normal,
        },
    ];
    let Some(result) = sync_on_ring(&targets) else {
        return Ok(());
    };
    let Err(e) = result else {
        panic!("a missing file was reported synced");
    };
    assert_eq!(e.error.kind(), crate::io::ErrorKind::NotFound);
    assert!(e.spent.is_empty(), "nothing was synced");
    Ok(())
}

/// Batches on one thread share its ring, a larger batch than the ring holds
/// gets a larger one, and a batch after a failed one syncs on a ring that
/// holds nothing of the failure: each reports its own result.
#[test]
fn batches_after_a_failure_and_a_larger_batch_still_sync() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let paths: Vec<std::path::PathBuf> =
        (0..20).map(|i| dir.path().join(format!("f{i}"))).collect();
    for path in &paths {
        std::fs::File::create(path)?.write_all(b"x")?;
    }
    fn target(path: &std::path::PathBuf) -> SyncTarget<'_> {
        SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        }
    }
    let small: Vec<SyncTarget<'_>> = paths.iter().take(2).map(target).collect();
    let Some(first) = sync_on_ring(&small) else {
        return Ok(());
    };
    first?;
    let missing = dir.path().join("missing");
    let present = dir.path().join("f0");
    let failing = [target(&present), target(&missing)];
    // A target that does not open was not synced: no sync is spent.
    assert!(matches!(
        sync_on_ring(&failing),
        Some(Err(failure)) if failure.spent.is_empty()
    ));
    let large: Vec<SyncTarget<'_>> = paths.iter().map(target).collect();
    assert!(matches!(sync_on_ring(&large), Some(Ok(()))));
    assert!(matches!(sync_on_ring(&small), Some(Ok(()))));
    Ok(())
}

/// A batch of more files than the process may hold open syncs: it goes to
/// the ring in bounded chunks, and a chunk the descriptors run out for goes
/// in halves. Guards against a persist of many deferred flushes failing with
/// `EMFILE` on every retry.
#[test]
fn a_batch_larger_than_the_descriptor_limit_syncs() -> crate::Result<()> {
    const FILES: usize = 400;
    // The limit is the process's, and a harness may run other tests on its
    // threads: the test lowers it only in a child of its own.
    const CHILD: &str = "LSM_TREE_DESCRIPTOR_LIMIT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let name = "fs::std_fs::ring_sync::tests::a_batch_larger_than_the_descriptor_limit_syncs";
        // The child runs the way cargo ran this test: through the runner of
        // the exact triple this binary was built for when one is set (an
        // emulator for a cross build, whose binary the host cannot exec),
        // directly otherwise. A runner a cargo config names reaches no
        // process cargo starts, and none is named in this repository's.
        let triple = env!("LSM_TREE_TARGET")
            .to_ascii_uppercase()
            .replace(['-', '.'], "_");
        let runner = std::env::var(format!("CARGO_TARGET_{triple}_RUNNER")).ok();
        let mut runner_words = runner.iter().flat_map(|value| value.split_whitespace());
        let mut command = match runner_words.next() {
            Some(program) => {
                let mut command = std::process::Command::new(program);
                command.args(runner_words).arg(std::env::current_exe()?);
                command
            }
            None => std::process::Command::new(std::env::current_exe()?),
        };
        let child = command.args(["--exact", name]).env(CHILD, "1").output()?;
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(
            child.status.success() && stdout.contains("1 passed"),
            "the child ran the test and it passed: {stdout}{}",
            String::from_utf8_lossy(&child.stderr)
        );
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    let paths: Vec<std::path::PathBuf> = (0..FILES)
        .map(|i| dir.path().join(format!("f{i}")))
        .collect();
    for path in &paths {
        std::fs::File::create(path)?.write_all(b"x")?;
    }
    let targets: Vec<SyncTarget<'_>> = paths
        .iter()
        .map(|path| SyncTarget::FileData {
            path,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        })
        .collect();
    // Room for what this process holds open and a quarter of a chunk, not for
    // the batch nor a whole chunk: the chunks run out of descriptors and are
    // synced in halves.
    let open_now = std::fs::read_dir("/proc/self/fd")?.count() as u64;
    let (soft, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
    rlimit::setrlimit(rlimit::Resource::NOFILE, open_now + 24, hard)?;
    let result = sync_on_ring(&targets);
    rlimit::setrlimit(rlimit::Resource::NOFILE, soft, hard)?;
    let Some(result) = result else {
        return Ok(());
    };
    result?;
    Ok(())
}

/// A sync the kernel refuses for its target fails the batch with that target
/// spent, once every entry of the batch is reaped, and leaves the ring on:
/// `/dev/null` takes no data sync. Guards against one such target being taken
/// for a ring that runs no sync.
#[test]
fn a_sync_the_kernel_refuses_fails_the_batch() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("f");
    std::fs::File::create(&file)?.write_all(b"x")?;
    let null = std::path::Path::new("/dev/null");
    let regular = SyncTarget::FileData {
        path: &file,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    };
    // The ring is known to sync a regular file, or there is nothing to test.
    let Some(alone) = sync_on_ring(core::slice::from_ref(&regular)) else {
        return Ok(());
    };
    alone?;
    let targets = [
        regular,
        SyncTarget::FileData {
            path: null,
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        },
    ];
    let Some(Err(failure)) = sync_on_ring(&targets) else {
        panic!("the refused sync fails the batch through the ring");
    };
    assert_eq!(failure.spent, [1], "the target that has no sync is spent");
    assert!(
        matches!(sync_on_ring(core::slice::from_ref(&regular)), Some(Ok(()))),
        "the ring still syncs"
    );
    Ok(())
}

/// Only a ring setup the kernel refuses for good (no `io_uring`, or one a
/// filter or sysctl keeps from the process) turns the ring off for the
/// thread; short memory or descriptors may pass. Guards against a thread
/// giving up the ring over a passing shortage, or asking again on every batch
/// for a ring it will never get.
#[test]
fn only_a_lasting_refusal_of_a_ring_turns_it_off() {
    assert!(super::refuses_setup(libc::ENOSYS));
    assert!(super::refuses_setup(libc::EPERM));
    assert!(!super::refuses_setup(libc::ENOMEM));
    assert!(!super::refuses_setup(libc::EMFILE));
}

/// A wait that fails before any sync is reaped, with none failed, fails the
/// batch with the wait's own error and spends what the kernel took. Guards
/// against such a batch reporting no error, or a sync error it never had.
#[test]
fn a_batch_cut_short_before_any_completion_fails_with_the_wait() {
    let failure = super::Completions::default()
        .cut_short(std::io::Error::other("the wait failed"), [0, 2].into_iter());
    assert!(
        failure.error.to_string().contains("the wait failed"),
        "{}",
        failure.error
    );
    assert_eq!(failure.spent, [0, 2]);
}

/// A kept handle with no descriptor is refused by the ring's open rather
/// than synced through some other descriptor. Guards the open the ring path
/// reaches only past the batch's own check.
#[test]
fn a_kept_handle_without_a_descriptor_does_not_open_for_the_ring() -> crate::Result<()> {
    use crate::fs::{FsOpenOptions, MemFs};

    let memory = MemFs::new();
    let path = std::path::Path::new("/m");
    let file = memory.open(path, &FsOpenOptions::new().write(true).create_new(true))?;
    let opened = super::open(&SyncTarget::OpenFile {
        file: &*file,
        path,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    });
    assert!(opened.is_err(), "no descriptor to sync through");
    Ok(())
}

/// A batch whose every sync the ring answers as it would an opcode it does
/// not run goes another way, and the thread asks no ring again: each
/// `/dev/null` takes no data sync, which is that answer. Guards against such
/// a batch being reported synced, or failed, rather than synced elsewhere.
#[test]
fn a_batch_every_sync_refused_goes_another_way() -> crate::Result<()> {
    use super::NO_RING_SYNC;

    NO_RING_SYNC.set(false);
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("f");
    std::fs::File::create(&file)?.write_all(b"x")?;
    let regular = SyncTarget::FileData {
        path: &file,
        origin: WriteOrigin::FlushTable,
        mode: SyncMode::Normal,
    };
    // The ring is known to sync a regular file, or there is nothing to test.
    let Some(alone) = sync_on_ring(core::slice::from_ref(&regular)) else {
        return Ok(());
    };
    alone?;
    // On a thread of its own: this one proved its ring runs the opcode.
    let (went_another_way, turned_off) = std::thread::spawn(|| {
        let null = SyncTarget::FileData {
            path: std::path::Path::new("/dev/null"),
            origin: WriteOrigin::FlushTable,
            mode: SyncMode::Normal,
        };
        let through_ring = sync_on_ring(&[null, null]);
        (through_ring.is_none(), NO_RING_SYNC.get())
    })
    .join()
    .map_err(|_| crate::Error::Unrecoverable)?;
    assert!(went_another_way, "the batch goes another way");
    assert!(turned_off, "the thread asks no ring again");
    Ok(())
}

/// A folder target that names a file is refused, as
/// `StdFs::sync_directory_with` refuses it: the ring path does not sync a
/// file in place of the folder a caller meant to make durable.
#[test]
fn a_folder_target_naming_a_file_is_refused() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("file");
    std::fs::File::create(&file)?;
    let targets = [
        SyncTarget::Folder {
            path: &file,
            mode: SyncMode::Normal,
        },
        SyncTarget::Folder {
            path: dir.path(),
            mode: SyncMode::Normal,
        },
    ];
    let Some(result) = sync_on_ring(&targets) else {
        return Ok(());
    };
    let Err(e) = result else {
        panic!("a file was synced as a folder");
    };
    assert_eq!(e.error.kind(), crate::io::ErrorKind::InvalidInput);
    Ok(())
}
