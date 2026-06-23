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
