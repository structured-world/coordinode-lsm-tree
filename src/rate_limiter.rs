// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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

// no_std-ready: the token-bucket core (this constant, `Bucket`, the limiter's
// rate fields and `acquire_wait`) is clock-agnostic, but the no_std
// `request_interruptible` is a passthrough until a caller injects a monotonic
// clock, so under no_std these are present-but-unused rather than dead.
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-ready token-bucket core; awaits a clock-injecting no_std caller"
    )
)]
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Mutable bucket state, guarded by a single lock.
#[derive(Debug)]
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-ready token-bucket state; awaits a clock-injecting no_std caller"
    )
)]
struct Bucket {
    /// Available budget in bytes. May go negative (debt) when a request
    /// debits more than is currently available; the deficit is what the
    /// caller waits out.
    available: i64,
    /// Monotonic time of the last refill, as nanoseconds since the
    /// limiter's origin.
    last_refill_nanos: u128,
}

#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-ready token-bucket state; awaits a clock-injecting no_std caller"
    )
)]
impl Bucket {
    /// A bucket holding one second of `rate`, refilled as of `now_nanos`, so
    /// the first request is not penalised.
    fn full(rate: u64, now_nanos: u128) -> Self {
        Self {
            available: i64::try_from(rate).unwrap_or(i64::MAX),
            last_refill_nanos: now_nanos,
        }
    }

    /// Adds what `rate` accrued since the last refill, capped at one second
    /// of it. `saturating_sub` guards against a non-monotonic `now` (the
    /// clock should be monotonic, but a backwards step must not underflow).
    fn refill(&mut self, rate: u64, now_nanos: u128) {
        let elapsed = now_nanos.saturating_sub(self.last_refill_nanos);
        if elapsed == 0 {
            return;
        }
        let refilled = elapsed.saturating_mul(u128::from(rate)) / NANOS_PER_SEC;
        // Fits in i64 for any realistic elapsed/rate; clamped so an absurd
        // elapsed cannot overflow the add.
        let refilled = i64::try_from(refilled).unwrap_or(i64::MAX);
        self.available = self.available.saturating_add(refilled);
        self.cap(rate);
        self.last_refill_nanos = now_nanos;
    }

    /// Drops credit above one second of `rate`; debt is left as it is.
    fn cap(&mut self, rate: u64) {
        let ceiling = i64::try_from(rate).unwrap_or(i64::MAX);
        if self.available > ceiling {
            self.available = ceiling;
        }
    }
}

/// Compaction I/O rate limiter (leaky token bucket).
///
/// One limiter bounds every compaction that holds it: a tree builds its own
/// from [`Config::compaction_rate_limit`](crate::Config::compaction_rate_limit),
/// or several trees are handed one through
/// [`Config::compaction_rate_limiter`](crate::Config::compaction_rate_limiter)
/// and share its budget. The limiter owns the rate, so a retune through
/// [`set_rate`](Self::set_rate) is seen by every holder on its next request.
///
/// A rate of `0` disables throttling entirely: every request returns
/// immediately. The bucket holds at most one second of the current rate, so
/// an idle limiter grants a one-second burst and no more.
///
/// **Fairness.** Each caller debits the bucket per item and sleeps out its
/// own deficit, so a long compaction yields between items rather than
/// holding a reservation. Beyond that there is no ordering between callers:
/// whoever debits first pays first.
///
/// # Examples
///
/// ```
/// use lsm_tree::rate_limiter::RateLimiter;
/// use std::time::Duration;
///
/// let limiter = RateLimiter::new(1_000);
/// // The first second of rate is available at once.
/// assert_eq!(limiter.acquire_wait(1_000, Duration::ZERO), Duration::ZERO);
/// // Raising the rate shortens what the next request owes.
/// limiter.set_rate_at(2_000, Duration::ZERO);
/// assert_eq!(
///     limiter.acquire_wait(1_000, Duration::ZERO),
///     Duration::from_millis(500),
/// );
/// ```
#[derive(Debug)]
pub struct RateLimiter {
    /// Refill rate in bytes per second. `0` means unlimited (disabled).
    /// Written only under the bucket lock, so a request that holds the lock
    /// sees the rate its bucket state was settled against.
    rate_bytes_per_sec: AtomicU64,
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
            bucket: Mutex::new(Bucket::full(rate_bytes_per_sec, 0)),
        }
    }

    /// The current rate in bytes per second; `0` when throttling is off.
    #[must_use]
    pub fn rate(&self) -> u64 {
        self.rate_bytes_per_sec.load(Ordering::Relaxed)
    }

    /// Changes the rate at monotonic time `now`, for every holder of this
    /// limiter; the next request is measured against it.
    ///
    /// What the bucket holds across the change:
    ///
    /// - time up to `now` refills at the old rate;
    /// - credit above one second of the new rate is dropped, so a lower rate
    ///   does not keep an old, larger burst;
    /// - debt is owed bytes and stays owed, repaid at the new rate;
    /// - switching on from `0` starts a fresh bucket, one second of the new
    ///   rate, as [`new`](Self::new) does: the unthrottled time earns no
    ///   credit;
    /// - switching to `0` makes every request immediate, and a caller already
    ///   sleeping in [`request_interruptible`](Self::request_interruptible)
    ///   stops sleeping.
    ///
    /// `now` is on the same clock as [`acquire_wait`](Self::acquire_wait);
    /// with the `std` feature, [`set_rate`](Self::set_rate) reads it.
    pub fn set_rate_at(&self, bytes_per_sec: u64, now: Duration) {
        let now_nanos = now.as_nanos();
        let mut bucket = self.bucket.lock();
        let old = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if old == bytes_per_sec {
            return;
        }
        if old == 0 {
            *bucket = Bucket::full(bytes_per_sec, now_nanos);
        } else {
            bucket.refill(old, now_nanos);
            if bytes_per_sec != 0 {
                bucket.cap(bytes_per_sec);
            }
        }
        self.rate_bytes_per_sec
            .store(bytes_per_sec, Ordering::Relaxed);
    }

    /// Changes the rate now; see [`set_rate_at`](Self::set_rate_at) for what
    /// the bucket holds across the change.
    // no-std: set_rate_at with a caller-provided monotonic clock
    #[cfg(feature = "std")]
    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.set_rate_at(bytes_per_sec, Self::std_now());
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
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "no_std-ready token-bucket decision; awaits a clock-injecting no_std caller"
        )
    )]
    pub fn acquire_wait(&self, bytes: u64, now: Duration) -> Duration {
        if self.rate_bytes_per_sec.load(Ordering::Relaxed) == 0 {
            return Duration::ZERO;
        }
        let now_nanos = now.as_nanos();

        let mut bucket = self.bucket.lock();
        // Read again under the lock: the rate the bucket was last settled
        // against, which a concurrent change may have moved since the check
        // above.
        let rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if rate == 0 {
            return Duration::ZERO;
        }
        let rate_u128 = u128::from(rate);
        bucket.refill(rate, now_nanos);

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
        let mut rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        let mut remaining = self.acquire_wait(bytes, Self::std_now());
        while !remaining.is_zero() {
            if should_stop() {
                return true;
            }
            // A rate changed mid-wait applies to what is still owed: switched
            // off, the caller goes; otherwise the rest is repaid at the new
            // rate (owed bytes = remaining time × old rate).
            let now_rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
            if now_rate == 0 {
                return false;
            }
            if now_rate != rate {
                remaining = Self::rescale(remaining, rate, now_rate);
                rate = now_rate;
                continue;
            }
            let chunk = remaining.min(Self::POLL_INTERVAL);
            std::thread::sleep(chunk);
            remaining = remaining.saturating_sub(chunk);
        }
        false
    }

    /// `remaining` owed at `from` bytes/s, expressed at `to` bytes/s. Both
    /// rates are nonzero.
    #[cfg(feature = "std")]
    fn rescale(remaining: Duration, from: u64, to: u64) -> Duration {
        let nanos = remaining.as_nanos().saturating_mul(u128::from(from)) / u128::from(to);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
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
mod tests;
