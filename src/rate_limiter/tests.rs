use super::*;
use test_log::test;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[test]
fn zero_rate_disables_throttling() {
    let rl = RateLimiter::new(0);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000_000, ms(0)));
}

#[test]
fn within_initial_burst_proceeds_immediately() {
    // 1000 B/s rate → 1000 B initial burst. A 1000 B request at t=0
    // exactly drains the burst with no wait.
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
}

#[test]
fn overdraft_waits_proportional_to_deficit() {
    // 1000 B/s. Burst 1000 B. First request drains the burst (no
    // wait); a second 500 B request at the same instant goes 500 B
    // into debt → must wait 500 B / 1000 B/s = 500 ms.
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    assert_eq!(ms(500), rl.acquire_wait(500, ms(0)));
}

#[test]
fn refill_accrues_over_time() {
    // 1000 B/s. Drain the burst at t=0, then at t=500ms the bucket has
    // refilled 500 B, so a 500 B request proceeds with no wait.
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    assert_eq!(Duration::ZERO, rl.acquire_wait(500, ms(500)));
}

#[test]
fn burst_is_capped_at_one_second_of_rate() {
    // 1000 B/s → burst ceiling 1000 B. Idle for 10 s; the bucket must
    // NOT accumulate 10 000 B. A 1000 B request drains the capped
    // burst (no wait); a further 1000 B at the same instant goes fully
    // into debt → 1000 ms wait, proving accumulation was capped.
    let rl = RateLimiter::new(1_000);
    assert_eq!(
        Duration::ZERO,
        rl.acquire_wait(1_000, Duration::from_secs(10))
    );
    assert_eq!(
        Duration::from_secs(1),
        rl.acquire_wait(1_000, Duration::from_secs(10))
    );
}

#[test]
fn sustained_rate_holds_at_configured_throughput() {
    // Issue 1000 B every 1000 ms against a 1000 B/s limit: after the
    // initial burst each request proceeds with zero wait (steady state
    // at exactly the rate).
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    for sec in 1..=5 {
        assert_eq!(
            Duration::ZERO,
            rl.acquire_wait(1_000, Duration::from_secs(sec)),
            "steady-state request at second {sec} should not wait"
        );
    }
}

#[cfg(feature = "std")]
#[test]
fn request_interruptible_bails_out_before_sleeping_when_stopped() {
    // 1 B/s with a 1 MiB request implies an ~12-day wait. With
    // should_stop already true, the call must return `true` immediately
    // (the stop check precedes the first sleep) rather than blocking —
    // this is what keeps shutdown responsive under a low rate limit.
    let rl = RateLimiter::new(1);
    let start = std::time::Instant::now();
    let stopped = rl.request_interruptible(1_024 * 1_024, || true);
    assert!(stopped, "should report it was interrupted");
    assert!(
        start.elapsed() < ms(500),
        "must not sleep the full computed wait when stopped"
    );
}

#[cfg(feature = "std")]
#[test]
fn request_interruptible_zero_rate_is_immediate_passthrough() {
    let rl = RateLimiter::new(0);
    let start = std::time::Instant::now();
    let stopped = rl.request_interruptible(1_000_000, || false);
    assert!(!stopped, "rate 0 never throttles, so never interrupted");
    assert!(start.elapsed() < ms(500), "rate 0 must not sleep");
}

/// A new rate is what the next request is measured against: with the burst
/// drained, a 1000 B request at 2000 B/s owes half a second, not one.
#[test]
fn a_raised_rate_takes_effect_on_the_next_request() {
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    rl.set_rate_at(2_000, ms(0));
    assert_eq!(2_000, rl.rate());
    assert_eq!(ms(500), rl.acquire_wait(1_000, ms(0)));
}

/// The burst ceiling is one second of the CURRENT rate: after a raise from
/// 1000 to 5000 B/s, ten idle seconds refill 5000 B, not the old 1000 B.
#[test]
fn a_raised_rate_raises_the_burst_ceiling() {
    let rl = RateLimiter::new(1_000);
    rl.set_rate_at(5_000, Duration::from_secs(10));
    assert_eq!(
        Duration::ZERO,
        rl.acquire_wait(5_000, Duration::from_secs(20))
    );
    assert_eq!(
        Duration::from_secs(1),
        rl.acquire_wait(5_000, Duration::from_secs(20))
    );
}

/// Credit above the new ceiling is dropped on a lower rate: a full 1000 B
/// bucket lowered to 100 B/s keeps 100 B, so the second 100 B request waits
/// a full second.
#[test]
fn a_lowered_rate_drops_credit_above_the_new_ceiling() {
    let rl = RateLimiter::new(1_000);
    rl.set_rate_at(100, ms(0));
    assert_eq!(Duration::ZERO, rl.acquire_wait(100, ms(0)));
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(100, ms(0)));
}

/// Debt is owed bytes and stays owed across a change: 1000 B of debt at
/// 1000 B/s is a second; at 2000 B/s the same debt is half a second.
#[test]
fn debt_is_kept_in_bytes_across_a_rate_change() {
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(1_000, ms(0)));
    rl.set_rate_at(2_000, ms(0));
    // A zero-byte request reports what is still owed.
    assert_eq!(ms(500), rl.acquire_wait(0, ms(0)));
}

/// Settling up to the change uses the old rate: half a second at 1000 B/s
/// refills 500 B, carried into the 2000 B/s bucket.
#[test]
fn time_before_a_change_refills_at_the_old_rate() {
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    rl.set_rate_at(2_000, ms(500));
    assert_eq!(Duration::ZERO, rl.acquire_wait(500, ms(500)));
    assert_eq!(ms(250), rl.acquire_wait(500, ms(500)));
}

/// A limiter switched on from 0 starts as if built at that moment: a
/// one-second burst of the new rate and no credit for the unthrottled time.
#[test]
fn switching_on_from_zero_starts_a_fresh_one_second_burst() {
    let rl = RateLimiter::new(0);
    rl.set_rate_at(1_000, Duration::from_secs(100));
    assert_eq!(
        Duration::ZERO,
        rl.acquire_wait(1_000, Duration::from_secs(100))
    );
    assert_eq!(
        Duration::from_secs(1),
        rl.acquire_wait(1_000, Duration::from_secs(100))
    );
}

/// A long unthrottled period between two throttled ones earns nothing: the
/// bucket switched back on holds one second of rate, not the idle time.
#[test]
fn an_unthrottled_period_grants_no_credit() {
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(1_000, ms(0)));
    rl.set_rate_at(0, ms(0));
    rl.set_rate_at(1_000, Duration::from_secs(1_000));
    assert_eq!(
        Duration::ZERO,
        rl.acquire_wait(1_000, Duration::from_secs(1_000))
    );
    assert_eq!(
        Duration::from_secs(1),
        rl.acquire_wait(1_000, Duration::from_secs(1_000))
    );
}

/// Switched off, every request is immediate, whatever debt the bucket held.
#[test]
fn switching_off_makes_requests_immediate() {
    let rl = RateLimiter::new(1_000);
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000, ms(0)));
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(1_000, ms(0)));
    rl.set_rate_at(0, ms(0));
    assert_eq!(0, rl.rate());
    assert_eq!(Duration::ZERO, rl.acquire_wait(1_000_000, ms(0)));
}

/// Runs `work` on its own thread; the returned closure takes its result, and
/// fails the test once `bound` passes instead of hanging on a wake-up that
/// never comes.
#[cfg(feature = "std")]
fn spawn_bounded<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> impl FnOnce(Duration) -> T {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::spawn(move || {
        tx.send(work()).expect("the test waits for this result");
    });
    move |bound| {
        let result = rx
            .recv_timeout(bound)
            .expect("the waiter must finish within its bound");
        handle.join().unwrap();
        result
    }
}

/// A caller already sleeping out a debt is released when throttling is
/// switched off, instead of sleeping the wait computed at the old rate.
#[cfg(feature = "std")]
#[test]
fn switching_off_releases_a_caller_mid_wait() {
    let rl = alloc::sync::Arc::new(RateLimiter::new(1));
    let waiter = {
        let rl = alloc::sync::Arc::clone(&rl);
        spawn_bounded(move || {
            let start = std::time::Instant::now();
            // 1 B/s: roughly an hour of debt.
            let stopped = rl.request_interruptible(3_600, || false);
            (stopped, start.elapsed())
        })
    };
    std::thread::sleep(ms(300));
    rl.set_rate(0);
    let (stopped, took) = waiter(Duration::from_secs(5));
    assert!(!stopped, "a released caller proceeds, it is not stopped");
    assert!(took < Duration::from_secs(5), "released after {took:?}");
}

/// A caller mid-wait pays the rest of its debt at the new rate: a two-second
/// wait at 1000 B/s ends within a second once the rate is raised a
/// thousandfold.
#[cfg(feature = "std")]
#[test]
fn a_caller_mid_wait_finishes_at_the_new_rate() {
    let rl = alloc::sync::Arc::new(RateLimiter::new(1_000));
    let waiter = {
        let rl = alloc::sync::Arc::clone(&rl);
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            // 1000 B of burst, then 2000 B of debt: two seconds at 1000 B/s.
            let stopped = rl.request_interruptible(3_000, || false);
            (stopped, start.elapsed())
        })
    };
    std::thread::sleep(ms(150));
    rl.set_rate(1_000_000);
    let (stopped, took) = waiter.join().unwrap();
    assert!(!stopped);
    assert!(took < Duration::from_secs(1), "finished after {took:?}");
}

/// Switching throttling off releases a waiter even when it is switched back
/// on before the waiter looks again: the release is an event, not a rate
/// the waiter has to catch at 0.
#[cfg(feature = "std")]
#[test]
fn a_brief_switch_off_still_releases_a_caller_mid_wait() {
    let rl = alloc::sync::Arc::new(RateLimiter::new(1));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let rl = alloc::sync::Arc::clone(&rl);
        std::thread::spawn(move || {
            // 1 B/s: roughly an hour of debt.
            let stopped = rl.request_interruptible(3_600, || false);
            done_tx.send(stopped).unwrap();
        })
    };
    std::thread::sleep(ms(300));
    // Off and back on, well inside one sleep chunk.
    rl.set_rate(0);
    rl.set_rate(1);
    let stopped = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a switch off must release the waiter");
    assert!(!stopped, "a released caller proceeds, it is not stopped");
    waiter.join().unwrap();
}

/// A stop signal that is clear on the entry check and set from the first poll
/// of the wait on, so the request is stopped after it took its debit, with
/// no dependence on thread timing.
#[cfg(feature = "std")]
fn stop_after_the_debit() -> impl Fn() -> bool {
    let calls = core::cell::Cell::new(0u32);
    move || {
        calls.set(calls.get() + 1);
        calls.get() > 1
    }
}

/// A request abandoned mid-wait through `request_abortable` does no I/O, so
/// its debit is returned: the next caller on the limiter, possibly another
/// tree's, does not wait out work that never happened.
#[cfg(feature = "std")]
#[test]
fn an_aborted_request_returns_its_debit() {
    let rl = RateLimiter::new(1_000);
    // 1000 B of burst, then 5000 B of debt: five seconds at 1000 B/s.
    assert!(rl.request_abortable(6_000, stop_after_the_debit()));
    let owed = rl.acquire_wait(100, RateLimiter::std_now());
    assert!(
        owed < ms(500),
        "the aborted debit must not burden the next request (owes {owed:?})"
    );
}

/// A request stopped through `request_interruptible` keeps its debit: its
/// caller may still do the I/O, so the budget stays spent.
#[cfg(feature = "std")]
#[test]
fn an_interrupted_request_keeps_its_debit() {
    let rl = RateLimiter::new(1_000);
    assert!(rl.request_interruptible(6_000, stop_after_the_debit()));
    let owed = rl.acquire_wait(100, RateLimiter::std_now());
    assert!(
        owed > Duration::from_secs(4),
        "the kept debit is still owed by the next request (owes {owed:?})"
    );
}

/// A debit returned by an aborted request shortens the wait of a request
/// queued behind it, which is no longer charged for the returned bytes.
#[cfg(feature = "std")]
#[test]
fn an_aborted_debit_shortens_a_later_wait() {
    let rl = RateLimiter::new(1_000);
    // The first debit takes the burst and owes 4000 B; the second is queued
    // behind it and owes 5000 B.
    let first = rl.take_ticket(5_000, ms(0)).unwrap();
    let second = rl.take_ticket(1_000, ms(0)).unwrap();
    assert_eq!(Some(Duration::from_secs(5)), rl.remaining(&second, ms(0)));
    rl.withdraw(&first, 5_000);
    assert_eq!(
        None,
        rl.remaining(&second, ms(0)),
        "the later debit is covered once the earlier one is returned"
    );
}

/// A debit returned from behind a waiter does not release that waiter early:
/// it owes its own place in line, which the returned bytes never covered.
#[cfg(feature = "std")]
#[test]
fn an_aborted_debit_does_not_release_an_earlier_waiter() {
    let rl = RateLimiter::new(1_000);
    // The first debit takes the burst and owes 1000 B: one second.
    let first = rl.take_ticket(2_000, ms(0)).unwrap();
    let second = rl.take_ticket(5_000, ms(0)).unwrap();
    rl.withdraw(&second, 5_000);
    assert_eq!(
        Some(Duration::from_secs(1)),
        rl.remaining(&first, ms(0)),
        "the earlier debit still repays its own place in line"
    );
    assert_eq!(None, rl.remaining(&first, ms(1_000)));
}

/// A rate lowered in the middle of a wait applies from the moment it changed:
/// the part of the debt left at that moment is repaid at the new rate, not the
/// time slept so far counted at the old one.
#[cfg(feature = "std")]
#[test]
fn a_rate_lowered_mid_wait_applies_from_the_change() {
    let rl = RateLimiter::new(100_000);
    // The burst, then 10000 B of debt: 100 ms at 100000 B/s.
    let ticket = rl.take_ticket(110_000, ms(0)).unwrap();
    assert_eq!(Some(ms(100)), rl.remaining(&ticket, ms(0)));
    // 5000 B repaid by then; the other 5000 B at 10000 B/s take 500 ms.
    rl.set_rate_at(10_000, ms(50));
    assert_eq!(Some(ms(500)), rl.remaining(&ticket, ms(50)));
    assert_eq!(Some(ms(450)), rl.remaining(&ticket, ms(100)));
    assert_eq!(None, rl.remaining(&ticket, ms(550)));
}

/// A retune stamped earlier than a refill the bucket already counted does not
/// move the bucket's clock back: the interval between the two is repaid once,
/// not a second time as phantom credit.
#[test]
fn a_retune_stamped_before_the_last_refill_grants_no_phantom_credit() {
    let rl = RateLimiter::new(1_000);
    // Refilled as of 1 s, then 1000 B of debt: one second at 1000 B/s.
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(2_000, ms(1_000)));
    // A retune whose clock was read before that refill.
    rl.set_rate_at(2_000, ms(500));
    // Still at 1 s: the 1000 B are owed at 2000 B/s.
    assert_eq!(ms(500), rl.acquire_wait(0, ms(1_000)));
}

/// Voluntary context switches of the calling thread so far: each is a sleep
/// the thread went into.
#[cfg(target_os = "linux")]
fn thread_wakeups() -> libc::c_long {
    let mut usage = core::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the struct it is handed and nothing else;
    // `RUSAGE_THREAD` names the calling thread (Linux).
    let rc = unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage");
    // SAFETY: the successful call above filled it.
    unsafe { usage.assume_init() }.ru_nvcsw
}

/// A throttled wait that nothing disturbs sleeps until its deadline in one
/// go: it does not wake on a tick to look again.
#[cfg(all(feature = "std", target_os = "linux"))]
#[test]
fn an_undisturbed_wait_wakes_once_at_its_deadline() {
    let rl = RateLimiter::new(1_000);
    let before = thread_wakeups();
    let start = std::time::Instant::now();
    // The burst, then 1000 B of debt: one second at 1000 B/s.
    assert!(!rl.request_interruptible(2_000, || false));
    let took = start.elapsed();
    let woke = thread_wakeups() - before;
    assert!(took >= ms(900), "the debt is repaid first ({took:?})");
    assert!(woke <= 2, "a one-second wait went to sleep {woke} times");
}

/// A stop sent through the stop signal of a tree holding the limiter wakes a
/// request waiting on it at once, though its deadline is an hour away.
#[cfg(feature = "std")]
#[test]
fn a_stop_signal_wakes_a_request_waiting_on_the_limiter() {
    let rl = alloc::sync::Arc::new(RateLimiter::new(1));
    let signal = crate::stop_signal::StopSignal::default();
    signal.wake_on_stop(&rl);
    let waiter = {
        let rl = alloc::sync::Arc::clone(&rl);
        let signal = signal.clone();
        spawn_bounded(move || {
            let start = std::time::Instant::now();
            // 1 B/s: roughly an hour of debt.
            let stopped = rl.request_abortable(3_600, || signal.is_stopped());
            (stopped, start.elapsed())
        })
    };
    std::thread::sleep(ms(300));
    signal.send();
    let (stopped, took) = waiter(Duration::from_secs(5));
    assert!(stopped, "the stop ends the wait as a stop");
    assert!(took < Duration::from_secs(2), "stopped after {took:?}");
}

/// A wait longer than the monotonic clock can count to (`u64::MAX` bytes at
/// 1 B/s) sleeps without a deadline instead of panicking on it, and a stop
/// still ends it.
#[cfg(feature = "std")]
#[test]
fn a_wait_past_the_clock_range_sleeps_until_stopped() {
    let rl = alloc::sync::Arc::new(RateLimiter::new(1));
    let signal = crate::stop_signal::StopSignal::default();
    signal.wake_on_stop(&rl);
    let waiter = {
        let rl = alloc::sync::Arc::clone(&rl);
        let signal = signal.clone();
        spawn_bounded(move || rl.request_abortable(u64::MAX, || signal.is_stopped()))
    };
    std::thread::sleep(ms(300));
    signal.send();
    // A wait that panicked never reports, and one that ended on its own
    // reports `false`.
    assert!(
        waiter(Duration::from_secs(5)),
        "the stop ends the wait as a stop"
    );
}

/// A debit returned ahead of a waiter wakes it at once: its debt is covered,
/// and it does not sleep out the wait it had before the return.
#[cfg(feature = "std")]
#[test]
fn a_returned_debit_wakes_the_waiter_behind_it() {
    use core::sync::atomic::AtomicBool;

    let rl = alloc::sync::Arc::new(RateLimiter::new(1_000));
    let stop_first = alloc::sync::Arc::new(AtomicBool::new(false));
    // The burst, then 4000 B of debt: four seconds.
    let first = {
        let rl = alloc::sync::Arc::clone(&rl);
        let stop = alloc::sync::Arc::clone(&stop_first);
        std::thread::spawn(move || rl.request_abortable(5_000, || stop.load(Ordering::Acquire)))
    };
    std::thread::sleep(ms(150));
    // Queued behind it: five seconds while the first debit stands.
    let second = {
        let rl = alloc::sync::Arc::clone(&rl);
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let stopped = rl.request_interruptible(1_000, || false);
            (stopped, start.elapsed())
        })
    };
    std::thread::sleep(ms(150));
    // A flag of the caller's own: it rings the limiter itself.
    stop_first.store(true, Ordering::Release);
    rl.wake_waiters();
    assert!(first.join().unwrap(), "the first request stops");
    let (stopped, took) = second.join().unwrap();
    assert!(!stopped);
    assert!(
        took < Duration::from_secs(2),
        "released after {took:?}, not after the five seconds it owed behind the debit"
    );
}

/// A tree rings its limiter when it is dropped, opened new and reopened alike:
/// a compaction throttled to an hour-long wait ends at once.
#[cfg(feature = "std")]
#[test]
fn a_tree_stop_wakes_its_throttled_compaction() -> crate::Result<()> {
    use crate::{Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let open = || -> crate::Result<crate::Tree> {
        let crate::AnyTree::Standard(tree) = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .compaction_rate_limit(1)
        .open()?
        else {
            panic!("a standard tree");
        };
        Ok(tree)
    };
    // Created by the first pass, recovered by the second.
    for _ in 0..2 {
        let tree = open()?;
        let rl = alloc::sync::Arc::clone(&tree.compaction_rate_limiter);
        let signal = tree.stop_signal.clone();
        let waiter = spawn_bounded(move || {
            let start = std::time::Instant::now();
            let stopped = rl.request_abortable(3_600, || signal.is_stopped());
            (stopped, start.elapsed())
        });
        std::thread::sleep(ms(300));
        drop(tree);
        let (stopped, took) = waiter(Duration::from_secs(5));
        assert!(stopped);
        assert!(took < Duration::from_secs(2), "stopped after {took:?}");
    }
    Ok(())
}

/// A limiter at 1 MB/s backing off above a 10 ms ceiling, stepping on every
/// sample.
fn backed_off_limiter() -> RateLimiter {
    let rl = RateLimiter::new(1_000_000);
    rl.set_latency_backoff_at(
        Some(LatencyBackoff::new(ms(10)).with_period(Duration::ZERO)),
        ms(0),
    );
    rl
}

/// Feeds `count` samples of `latency`, one millisecond apart from `*now`.
fn feed(rl: &RateLimiter, latency: Duration, count: u32, now: &mut Duration) {
    for _ in 0..count {
        *now += ms(1);
        rl.record_read_latency_at(latency, *now);
    }
}

/// Without a backoff, latency samples change nothing.
#[test]
fn latency_samples_without_a_backoff_change_nothing() {
    let rl = RateLimiter::new(1_000_000);
    rl.record_read_latency_at(Duration::from_secs(5), ms(1));
    assert_eq!(None, rl.latency_backoff());
    assert_eq!(1_000_000, rl.effective_rate());
}

/// An unthrottled limiter stays unthrottled whatever its reads take.
#[test]
fn latency_samples_leave_an_unthrottled_limiter_unthrottled() {
    let rl = RateLimiter::new(0);
    rl.set_latency_backoff_at(Some(LatencyBackoff::new(ms(1))), ms(0));
    rl.record_read_latency_at(Duration::from_secs(5), Duration::from_secs(1));
    assert_eq!(0, rl.effective_rate());
    assert_eq!(
        Duration::ZERO,
        rl.acquire_wait(1 << 30, Duration::from_secs(1))
    );
}

/// Above the ceiling the granted rate steps down every period, never below
/// the floor, and settles on it while the latency stays high.
#[test]
fn latency_above_the_ceiling_steps_down_to_the_floor() {
    let rl = backed_off_limiter();
    let mut now = ms(0);
    let mut previous = rl.effective_rate();
    for _ in 0..100 {
        feed(&rl, ms(40), 1, &mut now);
        let granted = rl.effective_rate();
        assert!(granted <= previous, "the rate rose under a slow device");
        assert!(
            granted >= 50_000,
            "the rate fell below the floor: {granted}"
        );
        previous = granted;
    }
    assert_eq!(50_000, rl.effective_rate(), "5% of the configured rate");
    assert_eq!(1_000_000, rl.rate(), "the configured rate is kept");
}

/// One step follows PARDA's law: halfway to where the ceiling would put it.
#[test]
fn one_step_moves_the_rate_halfway_to_the_ceiling() {
    let rl = backed_off_limiter();
    rl.record_read_latency_at(ms(40), ms(1));
    // 1 MB/s × (0.5 + 0.5 × 10/40)
    assert_eq!(625_000, rl.effective_rate());
}

/// Inside the hysteresis band the rate holds; below it, it climbs back to the
/// configured rate, and no higher.
#[test]
fn latency_in_the_band_holds_and_below_it_climbs_back() {
    let rl = backed_off_limiter();
    let mut now = ms(0);
    feed(&rl, ms(40), 5, &mut now);
    let backed_off = rl.effective_rate();
    assert!(backed_off < 1_000_000);

    // 9 ms is under the 10 ms ceiling and above its 8 ms band: once the
    // estimate settles there, the rate stops moving.
    feed(&rl, ms(9), 200, &mut now);
    let settled = rl.effective_rate();
    feed(&rl, ms(9), 50, &mut now);
    assert_eq!(
        settled,
        rl.effective_rate(),
        "the rate moved inside the band"
    );
    assert!(settled >= 50_000);

    feed(&rl, ms(1), 50, &mut now);
    assert_eq!(
        1_000_000,
        rl.effective_rate(),
        "the rate climbs back in full"
    );
}

/// The rate steps at most once per period, however many reads arrive.
#[test]
fn the_rate_steps_at_most_once_per_period() {
    let rl = RateLimiter::new(1_000_000);
    rl.set_latency_backoff_at(Some(LatencyBackoff::new(ms(10))), ms(0));
    for t in 1..100 {
        rl.record_read_latency_at(ms(40), ms(t));
    }
    assert_eq!(
        1_000_000,
        rl.effective_rate(),
        "stepped inside the first period"
    );
    rl.record_read_latency_at(ms(40), ms(100));
    let one_step = rl.effective_rate();
    assert!(one_step < 1_000_000, "no step once the period passed");
    rl.record_read_latency_at(ms(40), ms(150));
    assert_eq!(one_step, rl.effective_rate(), "two steps in one period");
}

/// Ceiling, floor and the backoff itself change on a live limiter: a higher
/// floor lifts the granted rate at once, removing the backoff restores the
/// configured rate, and a rate change scales by the share held.
#[test]
fn the_backoff_changes_on_a_live_limiter() {
    let rl = backed_off_limiter();
    let mut now = ms(0);
    feed(&rl, ms(1_000), 100, &mut now);
    assert_eq!(50_000, rl.effective_rate());

    rl.set_rate_at(2_000_000, now);
    assert_eq!(
        100_000,
        rl.effective_rate(),
        "the new rate at the held share"
    );

    let raised_floor = LatencyBackoff::new(ms(10))
        .with_period(Duration::ZERO)
        .with_floor(0.5);
    rl.set_latency_backoff_at(Some(raised_floor), now);
    assert_eq!(1_000_000, rl.effective_rate(), "lifted to the new floor");
    assert_eq!(Some(raised_floor), rl.latency_backoff());

    rl.set_latency_backoff_at(None, now);
    assert_eq!(2_000_000, rl.effective_rate(), "the configured rate again");
}

/// The bucket repays debt at the granted rate, not the configured one.
#[test]
fn the_bucket_runs_at_the_granted_rate() {
    let rl = RateLimiter::new(1_000);
    rl.set_latency_backoff_at(
        Some(
            LatencyBackoff::new(ms(10))
                .with_period(Duration::ZERO)
                .with_floor(0.5),
        ),
        ms(0),
    );
    rl.record_read_latency_at(Duration::from_secs(10), ms(0));
    assert_eq!(500, rl.effective_rate());
    // A burst of one second of the granted rate, then debt repaid at it.
    assert_eq!(Duration::ZERO, rl.acquire_wait(500, ms(0)));
    assert_eq!(Duration::from_secs(1), rl.acquire_wait(500, ms(0)));
}

/// When only the limiter's own I/O slows the device, backing off removes the
/// cause: the rate settles where the latency sits in the band, above the
/// floor, instead of collapsing.
#[test]
fn backing_off_own_load_settles_above_the_floor() {
    // Many reads per period, as a scrub issues them.
    let rl = RateLimiter::new(1_000_000);
    rl.set_latency_backoff_at(Some(LatencyBackoff::new(ms(10)).with_period(ms(20))), ms(0));
    let mut now = ms(0);
    let mut lowest = u64::MAX;
    for _ in 0..4_000 {
        // 1 ms of device time plus 30 ms at the full configured rate.
        #[expect(clippy::cast_precision_loss, reason = "rates below 2^53")]
        let share = rl.effective_rate() as f64 / rl.rate() as f64;
        let latency = Duration::from_secs_f64(0.030f64.mul_add(share, 0.001));
        feed(&rl, latency, 1, &mut now);
        lowest = lowest.min(rl.effective_rate());
    }
    assert!(lowest >= 50_000, "fell below the floor: {lowest}");
    // The band is 8..10 ms, which this device gives at 23%..30% of the rate.
    let settled = rl.effective_rate();
    assert!(
        (200_000..=320_000).contains(&settled),
        "settled at {settled}, outside the band the device allows"
    );
}

/// Reads served in no measurable time after a congested spell climb back by
/// bounded steps, and the rate ends at the configured one.
#[test]
fn a_zero_latency_sample_climbs_by_a_bounded_step() {
    let rl = backed_off_limiter();
    let mut now = ms(0);
    feed(&rl, ms(1_000), 100, &mut now);
    assert_eq!(50_000, rl.effective_rate());
    for _ in 0..200 {
        feed(&rl, Duration::ZERO, 1, &mut now);
    }
    assert_eq!(1_000_000, rl.effective_rate());
}

/// Out-of-range settings are clamped, and a value that is not a number keeps
/// the default.
#[test]
#[expect(
    clippy::float_cmp,
    reason = "clamping returns the bound itself, exactly"
)]
fn latency_backoff_settings_are_clamped() {
    let backoff = LatencyBackoff::new(ms(10))
        .with_floor(0.0)
        .with_hysteresis(2.0);
    assert_eq!(0.001, backoff.floor());
    assert_eq!(0.95, backoff.hysteresis());

    let backoff = LatencyBackoff::new(ms(10))
        .with_floor(f64::NAN)
        .with_hysteresis(f64::NAN);
    assert_eq!(LatencyBackoff::DEFAULT_FLOOR, backoff.floor());
    assert_eq!(LatencyBackoff::DEFAULT_HYSTERESIS, backoff.hysteresis());
}

#[test]
fn backwards_clock_step_does_not_underflow() {
    // A non-monotonic `now` (earlier than last_refill) must not panic
    // or grant phantom budget: the saturating_sub clamps elapsed to 0.
    let rl = RateLimiter::new(1_000);
    let _ = rl.acquire_wait(1_000, ms(1_000));
    // Step backwards to t=0: no refill, the 500 B debit goes straight
    // into debt.
    assert_eq!(ms(500), rl.acquire_wait(500, ms(0)));
}
