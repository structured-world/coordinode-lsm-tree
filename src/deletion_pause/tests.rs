use super::*;
use crate::fs::{Fs, MemFs};
use std::path::Path;

fn write_file(fs: &MemFs, path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let opts = crate::fs::FsOpenOptions::new().write(true).create(true);
    let mut f = fs.open(path, &opts).unwrap();
    f.write_all(bytes).unwrap();
}

/// Test-only barrier that lets a regression test suspend the
/// `Drop for Pause` exactly between `active.fetch_sub(1)` and
/// `self.inner.queue.lock()`. Production builds never reach
/// `wait()` because the call site is `#[cfg(test)]`-gated.
///
/// The barrier is single-shot per `arm()`: `arm()` installs a
/// receiver, the next `wait()` call blocks until `release()` sends
/// on the matching sender, then the receiver is consumed. Tests
/// that don't `arm()` get a no-op `wait()`.
pub(super) mod drain_barrier {
    use std::sync::Mutex;
    use std::sync::mpsc;

    static CHANNEL: Mutex<Option<mpsc::Receiver<()>>> = Mutex::new(None);

    /// Install a receiver. Returns the sender end; calling `send(())`
    /// (or letting the sender drop) lets the next `wait()` proceed.
    pub fn arm() -> mpsc::Sender<()> {
        let (tx, rx) = mpsc::channel();
        *CHANNEL.lock().unwrap() = Some(rx);
        tx
    }

    /// Block until the armed sender releases us, or return
    /// immediately if no sender is armed.
    pub fn wait() {
        // Hold the lock only long enough to TAKE the receiver, so
        // the spinning Drop holds nothing while it waits on the
        // channel — otherwise a deadlock with `arm()` is possible.
        let rx = CHANNEL.lock().unwrap().take();
        if let Some(rx) = rx {
            // Wait for the test thread's signal. Drop-send is also a
            // valid release (the recv() returns RecvError, which we
            // ignore — releasing on disarm is intentional).
            let _ = rx.recv();
        }
    }
}

#[test]
fn deletion_pause_defers_then_executes_removal() {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/file.sst").to_path_buf();
    write_file(&fs, &path, b"sst");
    let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

    let pause = DeletionPause::new_shared();
    let guard = pause.acquire();

    assert!(pause.try_enqueue(dyn_fs.clone(), path.clone()));
    assert!(
        fs.exists(&path).unwrap(),
        "file must still exist while paused"
    );

    drop(guard);
    assert!(
        !fs.exists(&path).unwrap(),
        "file must be removed after pause released"
    );
}

#[test]
fn enqueue_returns_false_when_inactive() {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/file.sst").to_path_buf();
    write_file(&fs, &path, b"x");
    let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

    let pause = DeletionPause::new_shared();
    assert!(!pause.try_enqueue(dyn_fs, path.clone()));
    assert!(fs.exists(&path).unwrap());
}

/// Regression test for the generation race in `Drop for Pause`.
///
/// Scenario the broken code allows:
///
/// 1. Thread A holds the only pause (`active == 1`).
/// 2. Thread A calls `fetch_sub(1)`, observing `prev == 1` (now `active == 0`).
/// 3. Before Thread A locks the queue, Thread B calls `acquire()`
///    (`active == 1`) and `try_enqueue` queues a fresh deletion.
/// 4. Thread A finally locks the queue and the original code does
///    `mem::take`, *executing* the deletion Thread B was supposed to
///    defer. Thread B's file vanishes despite an active pause.
///
/// The deterministic reproducer below uses two channels to pin the
/// invariant check at the exact moment when Thread B holds an active
/// pause and the queue contains its enqueued item. Without the fix,
/// A's drop would have already swept the queue and removed B's file
/// before B signalled `ready` — the survives-while-B-holds-pause
/// assertion fires. With the fix, A's drop bails out under the lock
/// (because B's `acquire` already incremented `active`) and the file
/// survives until B drops at the end.
/// The deterministic reproducer drives A's drop on its own thread
/// and uses the test-only `drain_barrier` to suspend A INSIDE Drop
/// — exactly between `active.fetch_sub(1)` and `queue.lock()`. B
/// then runs `acquire() + try_enqueue()` in that window, which is
/// the precise race CodeRabbit/Copilot called out. After B's work
/// is observable, the test releases the barrier so A's drain step
/// runs against `active > 0` and bails out — leaving B's file
/// intact for the assertion. Without the fix (no `active`-recheck
/// under the lock) A would drain B's enqueue and the file would
/// disappear before the assert.
#[test]
fn drain_does_not_steal_a_new_generation_queue() {
    use std::sync::mpsc;
    use std::thread;

    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/race.sst").to_path_buf();
    write_file(&fs, &path, b"keep-me");
    let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

    let pause = DeletionPause::new_shared();
    let a = pause.acquire();

    // Arm the in-Drop barrier. The next Pause::drop on the last
    // guard will block at the barrier wait point AFTER fetch_sub
    // and BEFORE the queue lock. The sender we get back is what
    // releases that block when we say so.
    let release_a_tx = drain_barrier::arm();

    // (in_window_tx, in_window_rx): A signals it has entered the
    // barrier wait — i.e. fetch_sub is done, active is 0, drain
    // step is suspended. B should NOT touch the pause until then.
    let (in_window_tx, in_window_rx) = mpsc::channel::<()>();
    // (b_ready_tx, b_ready_rx): B signals it has acquired the new
    // generation and enqueued its file; main thread can now check
    // the survives-the-race invariant.
    let (b_ready_tx, b_ready_rx) = mpsc::channel::<()>();
    // (release_b_tx, release_b_rx): main thread tells B to drop
    // its pause guard after the invariant has been verified.
    let (release_b_tx, release_b_rx) = mpsc::channel::<()>();

    // Thread A: drives drop(a) directly. Drop hits the barrier
    // wait, blocks, and we send `in_window_tx` to advertise that
    // we're suspended in the exact race window.
    let a_pause = Arc::clone(&pause);
    let a_thread = thread::spawn(move || {
        // We have to announce we're about to suspend BEFORE the
        // drop runs — the drop itself can't signal because it
        // blocks. The main thread spin-waits on the active
        // counter (below) to confirm A's fetch_sub really executed.
        in_window_tx.send(()).unwrap();
        drop(a);
        // After release the drain step runs and Drop returns.
        // We keep `a_pause` alive for the spin-wait above; it
        // doesn't influence the race.
        drop(a_pause);
    });

    // Wait until A is about to drop. Spin-wait until A's fetch_sub
    // has actually decremented `active` to 0 — that's how we know
    // A is now suspended INSIDE the barrier wait, having passed
    // step 1 and not yet reached step 4 (queue lock + recheck).
    in_window_rx.recv().unwrap();
    while pause.active.load(Ordering::Acquire) != 0 {
        core::hint::spin_loop();
    }

    // Thread B: now run acquire + try_enqueue while A is suspended.
    // Without the in-Drop barrier, this would race A by microseconds
    // and the test would be flaky / pass on broken code (the old
    // bug). With the barrier we're DETERMINISTICALLY between A's
    // fetch_sub and A's queue.lock().
    let b_pause = Arc::clone(&pause);
    let b_fs = Arc::clone(&dyn_fs);
    let b_path = path.clone();
    let b_thread = thread::spawn(move || {
        let _b = b_pause.acquire();
        assert!(b_pause.try_enqueue(b_fs, b_path));
        b_ready_tx.send(()).unwrap();
        release_b_rx.recv().unwrap();
        // Implicit drop here drains the queue.
    });

    // Wait until B has acquired + enqueued. Now release A's drop
    // so the drain step runs. Under the fix A sees `active > 0`
    // under the lock and returns without taking B's enqueue.
    b_ready_rx.recv().unwrap();
    release_a_tx.send(()).unwrap();
    a_thread.join().unwrap();

    // Invariant: B still holds an active pause and the file is
    // still in B's queue. The fix MUST have prevented A's drain
    // from removing it. Without the fix this assertion fires.
    assert!(
        fs.exists(&path).unwrap(),
        "file must survive while Thread B holds an active pause \
         (a's drain leaked into b's generation)",
    );
    release_b_tx.send(()).unwrap();

    b_thread.join().unwrap();

    // Sanity: after B drops too, the file is gone (B's drop
    // drained its own generation properly).
    assert!(
        !fs.exists(&path).unwrap(),
        "file should be removed after both pauses dropped",
    );
}

/// A deferred prefix reclaim must not be DISCARDED when the drain cannot prove
/// the file is exclusively ours — because a checkpoint linked it, or because the
/// link-count probe failed. Unlinking the checkpoint only drops the link count;
/// it does not free the prefix, since the live restricted table still holds that
/// inode. The space would then stay allocated until an unrelated future
/// compaction retires the table — under exactly the low-space condition that
/// chose the tight-space path. The intent is kept and retried instead.
///
/// Driven through a failing probe: `MemFs` reports every path as
/// singly-linked, so the shared-inode case cannot be built on it, and both cases
/// take the same arm.
#[test]
fn a_deferred_punch_survives_an_unprovable_link_count_and_is_retried() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/1").to_path_buf();
    write_file(&mem, &path, &[b'x'; 4096]);
    let faulty = Arc::new(FaultFs::new(mem.clone()));
    let dyn_fs: Arc<dyn Fs> = faulty.clone();

    faulty.injector().arm(FaultRule::new(
        FaultOp::HardLinkCount,
        Fault::Error(crate::io::ErrorKind::PermissionDenied),
    ));

    let pause = DeletionPause::new_shared();
    let guard = pause.acquire();
    assert!(pause.try_enqueue_punch(Arc::clone(&dyn_fs), path, vec![(0, 2048)]));

    drop(guard);
    assert_eq!(
        mem.punched_bytes(),
        0,
        "an unprovable link count must not be punched through",
    );
    assert!(
        pause.has_pending_reclaims(),
        "the reclaim is retained, not discarded: nothing else would ever free \
         the consumed prefix",
    );

    // The probe works again and the file is exclusively ours, so the retry
    // performs the reclaim that was held.
    faulty.injector().clear();
    pause.retry_pending_reclaims();
    assert_eq!(
        mem.punched_bytes(),
        2048,
        "the retained reclaim runs once the file can be proven exclusive",
    );
    assert!(
        !pause.has_pending_reclaims(),
        "a completed reclaim is not retained",
    );
}

/// A deferred reclaim probes the link count and then punches. Unlike a table's
/// `Drop`, the queued item holds no live version whose lifetime keeps a
/// checkpoint out of that gap: a checkpoint starting between the probe and the
/// punch hard-links the file, and the punch then zeroes the inode its supposedly
/// immutable snapshot shares. The whole probe-and-punch sequence must therefore
/// run inside the mutation window a checkpoint's link window excludes.
#[test]
fn a_deferred_punch_waits_for_an_open_checkpoint_link_window() {
    use std::sync::mpsc;

    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/1").to_path_buf();
    write_file(&mem, &path, &[b'x'; 4096]);
    let dyn_fs: Arc<dyn Fs> = Arc::new(mem.clone());

    let pause = DeletionPause::new_shared();
    let guard = pause.acquire();
    assert!(pause.try_enqueue_punch(Arc::clone(&dyn_fs), path, vec![(0, 2048)]));
    // The pause released with the probe unavailable would retain the reclaim;
    // here the file IS exclusive, so retain it by holding the pause instead and
    // driving the retry explicitly below.
    let checkpoint = pause.enter_link_window();

    let retrier = Arc::clone(&pause);
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        drop(guard); // releases the pause: the drain retries the reclaim
        retrier.retry_pending_reclaims();
        done_tx.send(()).ok();
    });

    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(250))
            .is_err(),
        "a deferred punch must not run while a checkpoint's link window is open",
    );
    assert_eq!(
        mem.punched_bytes(),
        0,
        "and it must not have punched the inode the checkpoint is linking",
    );

    drop(checkpoint);
    handle.join().expect("the retry thread finishes");
    assert_eq!(
        mem.punched_bytes(),
        2048,
        "once the link window closes the reclaim runs",
    );
}

/// A punch that FAILS mid-pass must not take the reclaim down with it. The pass
/// stops (the hole pattern stays classifiable for a sidecar-less repair), but
/// the failed extent and everything below it are retained: dropping them leaves
/// the consumed prefix allocated with nothing left to free it, exactly as a
/// discarded intent would.
#[test]
fn a_failed_punch_retains_the_extent_and_the_untried_remainder() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    let mem = MemFs::new();
    mem.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/1").to_path_buf();
    write_file(&mem, &path, &[b'x'; 4096]);
    let faulty = Arc::new(FaultFs::new(mem.clone()));
    let dyn_fs: Arc<dyn Fs> = faulty.clone();

    // Three extents, top-down. The SECOND fails, so the first lands and the
    // third is never attempted.
    faulty.injector().arm(
        FaultRule::new(
            FaultOp::PunchHole,
            Fault::Error(crate::io::ErrorKind::Interrupted),
        )
        .skip(1)
        .once(),
    );

    let pause = DeletionPause::new_shared();
    let guard = pause.acquire();
    assert!(pause.try_enqueue_punch(
        Arc::clone(&dyn_fs),
        path,
        vec![(3072, 1024), (2048, 1024), (1024, 1024)],
    ));
    drop(guard);

    assert_eq!(
        mem.punched_bytes(),
        1024,
        "the pass stops at the first failure, so only the top extent landed",
    );
    assert!(
        pause.has_pending_reclaims(),
        "the failed extent and the untried remainder are retained, or nothing \
         would ever free that space",
    );

    // The fault is one-shot, so the retry completes what was held.
    pause.retry_pending_reclaims();
    assert_eq!(
        mem.punched_bytes(),
        3072,
        "the retry reclaims the extents the failed pass left behind",
    );
    assert!(!pause.has_pending_reclaims(), "nothing left to retry");
}

#[test]
fn nested_pauses_only_release_on_last_drop() {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/d")).unwrap();
    let path = Path::new("/d/file.sst").to_path_buf();
    write_file(&fs, &path, b"x");
    let dyn_fs: Arc<dyn Fs> = Arc::new(fs.clone());

    let pause = DeletionPause::new_shared();
    let outer = pause.acquire();
    let inner = pause.acquire();

    assert!(pause.try_enqueue(dyn_fs, path.clone()));

    drop(inner);
    assert!(fs.exists(&path).unwrap(), "still paused by outer guard");

    drop(outer);
    assert!(
        !fs.exists(&path).unwrap(),
        "released after last guard dropped"
    );
}
