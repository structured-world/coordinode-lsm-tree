// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Compaction I/O rate limiter.
//!
//! Background compaction can saturate disk bandwidth and starve user
//! point reads / range scans, spiking their P99 latency. This limiter
//! caps the rate at which the compaction worker is allowed to issue I/O
//! so user traffic keeps its share of the device. It is invoked only from
//! the compaction path, so flush and user reads are never throttled —
//! they simply never call it.
//!
//! # Model
//!
//! A leaky token bucket measured in bytes. Each request debits the
//! bucket; when the bucket goes into debt the caller must wait long
//! enough for the configured refill rate to repay it, which serialises
//! compaction I/O down to `rate_bytes_per_sec`. A rate of `0` disables
//! throttling: every request is immediate (the default, so the limiter is
//! wired unconditionally and switched on via
//! [`Config::compaction_rate_limit`](crate::Config::compaction_rate_limit)).
//!
//! # Clock injection
//!
//! The core decision function [`RateLimiter::acquire_wait`] takes the
//! current monotonic time as a `Duration` since an arbitrary origin, so
//! it is pure (no syscalls), unit-testable without sleeping, and compiles
//! without `std`. The interruptible blocking wrapper
//! [`RateLimiter::request_interruptible`] (which reads the system clock and
//! sleeps in pollable chunks) is gated behind the `std` feature.
//!
//! A priority-class extension (flush / user I/O also debiting the bucket
//! but draining ahead of compaction) is a planned refinement; this
//! revision throttles compaction alone.

use core::sync::atomic::Ordering;
use core::time::Duration;

use portable_atomic::AtomicU64;

use spin::Mutex;

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Mutable bucket state, guarded by a single lock.
#[derive(Debug)]
struct Bucket {
    /// Available budget in bytes. May go negative (debt) when a request
    /// debits more than is currently available; the deficit is what the
    /// caller waits out.
    available: i64,
    /// Monotonic time of the last refill, as nanoseconds since the
    /// limiter's origin.
    last_refill_nanos: u128,
}

/// Compaction I/O rate limiter (leaky token bucket).
///
/// Share across compaction invocations by wrapping in `Arc`; the limiter
/// is `Sync` (all mutable state is behind a lock / atomics).
///
/// A `rate_bytes_per_sec` of `0` disables throttling entirely: every
/// request returns immediately.
#[derive(Debug)]
pub struct RateLimiter {
    /// Refill rate in bytes per second. `0` means unlimited (disabled).
    rate_bytes_per_sec: AtomicU64,
    /// Maximum positive budget the bucket may accumulate, in bytes: one
    /// second of rate, so an idle limiter grants a one-second burst but no
    /// more (prevents a long-idle compactor from dumping an unbounded
    /// backlog at full speed).
    burst_bytes: u64,
    bucket: Mutex<Bucket>,
}

impl RateLimiter {
    /// Creates a limiter refilling at `rate_bytes_per_sec`.
    ///
    /// `0` disables throttling (every request is immediate). The burst
    /// ceiling is one second of rate.
    #[must_use]
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        Self {
            rate_bytes_per_sec: AtomicU64::new(rate_bytes_per_sec),
            burst_bytes: rate_bytes_per_sec,
            bucket: Mutex::new(Bucket {
                // Start with a full one-second burst so the first request
                // after construction is not penalised.
                available: i64::try_from(rate_bytes_per_sec).unwrap_or(i64::MAX),
                last_refill_nanos: 0,
            }),
        }
    }

    /// Core decision: how long the caller must wait before issuing an I/O
    /// of `bytes`, given the current monotonic time `now` (a `Duration`
    /// since this limiter's origin).
    ///
    /// Returns [`Duration::ZERO`] when the request may proceed
    /// immediately (including when the rate is `0`). Performs no sleeping
    /// and reads no clock, so it is fully deterministic for a given `now`
    /// sequence and usable in `no_std` builds.
    #[must_use]
    pub fn acquire_wait(&self, bytes: u64, now: Duration) -> Duration {
        let rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if rate == 0 {
            return Duration::ZERO;
        }
        let now_nanos = now.as_nanos();
        let rate_u128 = u128::from(rate);

        let mut bucket = self.bucket.lock();

        // Refill: add the bytes that accrued since the last refill, then
        // cap at the burst ceiling. `saturating_sub` guards against a
        // non-monotonic `now` (clock should be monotonic, but never let a
        // backwards step underflow).
        let elapsed = now_nanos.saturating_sub(bucket.last_refill_nanos);
        if elapsed > 0 {
            let refilled = elapsed.saturating_mul(rate_u128) / NANOS_PER_SEC;
            // refilled fits in i64 for any realistic elapsed/rate; clamp
            // defensively so an absurd elapsed can't overflow the add.
            let refilled = i64::try_from(refilled).unwrap_or(i64::MAX);
            bucket.available = bucket.available.saturating_add(refilled);
            let burst_i64 = i64::try_from(self.burst_bytes).unwrap_or(i64::MAX);
            if bucket.available > burst_i64 {
                bucket.available = burst_i64;
            }
            bucket.last_refill_nanos = now_nanos;
        }

        // Debit the request. Going negative is the debt the caller pays
        // off by waiting.
        let debit = i64::try_from(bytes).unwrap_or(i64::MAX);
        bucket.available = bucket.available.saturating_sub(debit);

        if bucket.available >= 0 {
            return Duration::ZERO;
        }

        // Wait long enough for the refill rate to repay the deficit.
        let deficit = bucket.available.unsigned_abs();
        let wait_nanos = u128::from(deficit).saturating_mul(NANOS_PER_SEC) / rate_u128;
        Duration::from_nanos(u64::try_from(wait_nanos).unwrap_or(u64::MAX))
    }

    /// Interruptible blocking request: waits (sleeping the current thread)
    /// until an I/O of `bytes` may proceed, polling `should_stop` so a
    /// shutdown / drop can break a long wait promptly.
    ///
    /// Returns `true` if the caller should abort: either `should_stop` was
    /// already set on entry (stop pending before any work) or it fired during
    /// the wait. Returns `false` if the full wait elapsed (caller may
    /// proceed).
    ///
    /// The budget is debited at most once (via a single
    /// [`acquire_wait`](Self::acquire_wait)) — the early returns for
    /// `rate == 0` and for an already-pending stop skip the debit entirely.
    /// When a wait is computed it is slept in
    /// <= [`POLL_INTERVAL`](Self::POLL_INTERVAL) chunks with a `should_stop`
    /// check before each, so even a multi-gigabyte item under a low limit
    /// cannot stall shutdown for more than one poll interval. Re-calling
    /// `acquire_wait` in the loop would wrongly re-debit the bucket each
    /// iteration, so the wait is computed once up front.
    ///
    /// A no-op returning `false` when the rate is `0` (no clock read).
    /// Only with the `std` feature; `no_std` callers drive `acquire_wait`
    /// with their own clock + interruptible wait.
    // no-std: caller-provided clock + acquire_wait() + caller's wait/poll loop
    #[cfg(feature = "std")]
    pub fn request_interruptible(&self, bytes: u64, should_stop: impl Fn() -> bool) -> bool {
        // Short-circuit BEFORE any clock read so the unthrottled default
        // (rate 0) costs a single relaxed atomic load — the compaction
        // merge loop calls this per item.
        if self.rate_bytes_per_sec.load(Ordering::Relaxed) == 0 {
            return false;
        }
        // If a stop is already pending, bail before touching the bucket /
        // clock — no point debiting or locking on the shutdown path.
        if should_stop() {
            return true;
        }
        // Debit once, then sleep the computed wait in interruptible chunks.
        let mut remaining = self.acquire_wait(bytes, Self::std_now());
        while !remaining.is_zero() {
            if should_stop() {
                return true;
            }
            let chunk = remaining.min(Self::POLL_INTERVAL);
            std::thread::sleep(chunk);
            remaining = remaining.saturating_sub(chunk);
        }
        false
    }

    /// `no_std` variant: there is no ambient monotonic clock to throttle
    /// against, so this never sleeps. It still honors the caller's stop signal
    /// so a shutdown is observed promptly; rate limiting itself is a no-op.
    // no-std: wire a caller-provided clock + `acquire_wait` poll loop to restore
    // throttling.
    #[cfg(not(feature = "std"))]
    pub fn request_interruptible(&self, _bytes: u64, should_stop: impl Fn() -> bool) -> bool {
        should_stop()
    }

    /// Maximum single sleep span inside
    /// [`request_interruptible`](Self::request_interruptible): the upper
    /// bound on how long a stop signal can go unnoticed mid-throttle.
    #[cfg(feature = "std")]
    const POLL_INTERVAL: Duration = Duration::from_millis(100);

    /// Monotonic time since a process-global origin, for the `std`
    /// wrapper. A shared origin is fine: each limiter's bucket tracks its
    /// own `last_refill_nanos` against the same monotonic reference, so
    /// only the deltas matter.
    #[cfg(feature = "std")]
    fn std_now() -> Duration {
        use std::sync::OnceLock;
        // `OnceLock` / `Instant` are std-only, hence this helper (and
        // `request`) live behind the `std` gate; the no_std path uses
        // `acquire_wait` with a caller-supplied clock instead.
        static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
        ORIGIN.get_or_init(std::time::Instant::now).elapsed()
    }
}

#[cfg(test)]
mod tests {
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
}
