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
//! without `std`. The blocking wrappers
//! [`RateLimiter::request_interruptible`] and
//! [`RateLimiter::request_abortable`] (which read the system clock and
//! sleep in pollable chunks) are gated behind the `std` feature.
//!
//! # Sharing and retuning
//!
//! A tree builds its own limiter from
//! [`Config::compaction_rate_limit`](crate::Config::compaction_rate_limit),
//! or several trees share one handed to them through
//! [`Config::compaction_rate_limiter`](crate::Config::compaction_rate_limiter)
//! and are bounded by it together. The limiter owns the rate:
//! [`RateLimiter::set_rate`] changes it for every holder, with no restart.

use alloc::vec::Vec;
use core::sync::atomic::Ordering;
use core::time::Duration;

use portable_atomic::AtomicU64;

use spin::Mutex;

// no_std-ready: the token-bucket core (this constant, `Bucket`, the limiter's
// rate fields and `acquire_wait`) is clock-agnostic, but the no_std
// request wrappers are passthroughs until a caller injects a monotonic
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
///
/// Debits and repayments are running totals, so a debit has a place in line:
/// its position is the total debited once it is added, and it is repaid when
/// the credit accrued reaches that position. A waiter re-reads the totals on
/// every poll instead of keeping its own countdown, so everything that moves
/// the line under it (a rate change, a debit withdrawn ahead of it) is seen
/// on the next poll. Totals are in bytes of `u64` requests summed in `u128`,
/// which no process lives long enough to overflow.
#[derive(Debug)]
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "no_std-ready token-bucket state; awaits a clock-injecting no_std caller"
    )
)]
struct Bucket {
    /// Every byte ever debited, withdrawn debits included.
    debited: u128,
    /// Every byte repaid by the refill, plus the starting burst.
    credited: u128,
    /// Withdrawn debits still ahead of some waiter, as `(position, bytes)`:
    /// they shorten the line only for positions at or past `position`.
    withdrawn: Vec<(u128, u128)>,
    /// Withdrawn bytes that every waiter still in line is past.
    withdrawn_ahead_of_all: u128,
    /// Monotonic time the refill is counted from, as nanoseconds since the
    /// limiter's origin.
    last_refill_nanos: u128,
    /// Rate changes so far.
    changes: u64,
    /// The value of `changes` at the last switch to `0`. A waiter whose debit
    /// predates it is released, and its debit is gone with the old bucket,
    /// even if the rate was switched back on before it looked.
    last_off: u64,
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
        let mut bucket = Self {
            debited: 0,
            credited: 0,
            withdrawn: Vec::new(),
            withdrawn_ahead_of_all: 0,
            last_refill_nanos: 0,
            changes: 0,
            last_off: 0,
        };
        bucket.fill(rate, now_nanos);
        bucket
    }

    /// Bytes still owed by everyone in line.
    fn outstanding(&self) -> u128 {
        let withdrawn: u128 = self.withdrawn.iter().map(|&(_, bytes)| bytes).sum();
        self.debited - self.withdrawn_ahead_of_all - withdrawn
    }

    /// Where a debit at `position` stands in the line once the withdrawn
    /// debits ahead of it are taken out.
    fn standing(&self, position: u128) -> u128 {
        let ahead: u128 = self
            .withdrawn
            .iter()
            .filter(|&&(at, _)| at <= position)
            .map(|&(_, bytes)| bytes)
            .sum();
        // The debits withdrawn at or before `position` lie inside it. A
        // settled withdrawal may lie past it, but only once the credit
        // reached that withdrawal, and so this earlier position: it stands
        // at the front of the line, at 0.
        (position - ahead).saturating_sub(self.withdrawn_ahead_of_all)
    }

    /// Refills to one second of `rate` as of `now_nanos`, as a new bucket
    /// would hold, keeping the change counts.
    fn fill(&mut self, rate: u64, now_nanos: u128) {
        self.credited = self.outstanding() + u128::from(rate);
        self.advance_clock(now_nanos);
        self.settle_withdrawn();
    }

    /// Moves the refill clock to `now_nanos`, never back: a caller whose
    /// reading predates a refill already counted would otherwise have the
    /// time between the two repaid a second time.
    fn advance_clock(&mut self, now_nanos: u128) {
        self.last_refill_nanos = self.last_refill_nanos.max(now_nanos);
    }

    /// Adds what `rate` accrued since the last refill, capped at one second
    /// of it. Only whole bytes are credited; the time of a partial byte is
    /// carried to the next refill, so polling often cannot starve a low rate.
    fn refill(&mut self, rate: u64, now_nanos: u128) {
        // A backwards step of the clock accrues nothing rather than
        // underflowing: the clock should be monotonic, and the next forward
        // reading resumes from the recorded instant.
        let elapsed = now_nanos.saturating_sub(self.last_refill_nanos);
        if elapsed == 0 || rate == 0 {
            return;
        }
        // An absurd elapsed saturates here and is then capped at one second
        // of rate, which is what any refill that long comes to.
        let refilled = elapsed.saturating_mul(u128::from(rate)) / NANOS_PER_SEC;
        if refilled == 0 {
            return;
        }
        let ceiling = self.outstanding() + u128::from(rate);
        if self.credited + refilled >= ceiling {
            self.credited = ceiling;
            self.last_refill_nanos = now_nanos;
        } else {
            self.credited += refilled;
            // The time those bytes took, rounded up so no byte is paid twice;
            // never past `now_nanos`, since `refilled` was rounded down.
            let spent = (refilled * NANOS_PER_SEC).div_ceil(u128::from(rate));
            self.last_refill_nanos += spent;
        }
        self.settle_withdrawn();
    }

    /// Drops credit above one second of `rate`; debt is left as it is.
    fn cap(&mut self, rate: u64) {
        let ceiling = self.outstanding() + u128::from(rate);
        if self.credited > ceiling {
            self.credited = ceiling;
        }
    }

    /// Folds into `withdrawn_ahead_of_all` every withdrawn debit whose
    /// position the credit has passed: every waiter before it is repaid, so
    /// taking it out of their standing too changes no one's release.
    fn settle_withdrawn(&mut self) {
        let mut i = 0;
        while let Some(&(at, bytes)) = self.withdrawn.get(i) {
            if self.credited >= self.standing(at) {
                self.withdrawn.swap_remove(i);
                self.withdrawn_ahead_of_all += bytes;
            } else {
                i += 1;
            }
        }
    }

    /// Adds a debit of `bytes` and returns its position in line.
    fn debit(&mut self, bytes: u64) -> u128 {
        self.debited += u128::from(bytes);
        self.debited
    }

    /// How long a debit at `position` still waits at `rate`, rounded up to
    /// the nanosecond so a waiter never wakes before its last byte is repaid.
    fn wait_for(&self, position: u128, rate: u64) -> Duration {
        // Credit past the position means the debit is repaid: nothing owed.
        let owed = self.standing(position).saturating_sub(self.credited);
        let rate = u128::from(rate);
        // `owed % rate < rate <= u64::MAX`, so this product fits in u128 and
        // the quotient is at most one second.
        #[expect(clippy::cast_possible_truncation, reason = "at most NANOS_PER_SEC")]
        let nanos = ((owed % rate) * NANOS_PER_SEC).div_ceil(rate) as u64;
        // A wait past `u64::MAX` seconds is forever either way.
        let secs = u64::try_from(owed / rate).unwrap_or(u64::MAX);
        Duration::from_secs(secs).saturating_add(Duration::from_nanos(nanos))
    }
}

/// A debit waiting to be repaid: its place in line and the change count it
/// was taken at.
#[cfg(feature = "std")]
struct Ticket {
    position: u128,
    taken_at: u64,
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
/// **Fairness.** Debits are repaid in the order they are taken: a waiter is
/// released once the credit accrued covers every debit up to its own, and a
/// debit withdrawn by [`request_abortable`](Self::request_abortable) stops
/// delaying the waiters behind it. Each caller debits per item, so a long
/// compaction yields between items rather than holding a reservation.
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
    /// - debt is owed bytes and stays owed, repaid at the new rate, including
    ///   the debt of callers already waiting;
    /// - switching on from `0` starts a fresh bucket, one second of the new
    ///   rate, as [`new`](Self::new) does: the unthrottled time earns no
    ///   credit;
    /// - switching to `0` makes every request immediate, and a caller already
    ///   waiting in [`request_interruptible`](Self::request_interruptible) or
    ///   [`request_abortable`](Self::request_abortable) stops waiting.
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
            bucket.fill(bytes_per_sec, now_nanos);
        } else {
            bucket.refill(old, now_nanos);
            // Time the old rate did not turn into a whole byte is dropped
            // with it: it is not owed at the new rate.
            bucket.advance_clock(now_nanos);
            if bytes_per_sec != 0 {
                bucket.cap(bytes_per_sec);
            }
        }
        // One step per rate change: a u64 does not wrap.
        bucket.changes += 1;
        if bytes_per_sec == 0 {
            bucket.last_off = bucket.changes;
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
        let mut bucket = self.bucket.lock();
        let rate = self.rate();
        if rate == 0 {
            return Duration::ZERO;
        }
        bucket.refill(rate, now.as_nanos());
        let position = bucket.debit(bytes);
        bucket.wait_for(position, rate)
    }

    /// Waits (sleeping the current thread) until an I/O of `bytes` may
    /// proceed, polling `should_stop` so a shutdown can break a long wait
    /// promptly. Use it where the I/O may still happen after a stop; a caller
    /// that does no I/O once stopped uses
    /// [`request_abortable`](Self::request_abortable).
    ///
    /// Returns `true` if `should_stop` was set on entry or fired during the
    /// wait, `false` once the wait is over (the caller may proceed). A stop
    /// keeps the debit: the budget stays spent for the I/O the caller may
    /// still do.
    ///
    /// The wait is re-derived from the shared bucket on every poll, at most
    /// 100 ms apart, so a rate change, a switch off or a debit withdrawn
    /// ahead of this one takes effect within one poll. A no-op returning
    /// `false` when the rate is `0` (no clock read). Only with the `std`
    /// feature; `no_std` callers drive `acquire_wait` with their own clock.
    // no-std: caller-provided clock + acquire_wait() + caller's wait/poll loop
    #[cfg(feature = "std")]
    pub fn request_interruptible(&self, bytes: u64, should_stop: impl Fn() -> bool) -> bool {
        self.request(bytes, should_stop, false)
    }

    /// Waits like [`request_interruptible`](Self::request_interruptible), for
    /// a caller that does no I/O once stopped: a stop returns the debit, so
    /// the callers behind it, possibly other trees', do not wait out work
    /// that never happened.
    ///
    /// Returns `true` if the caller must abort, `false` once it may proceed.
    // no-std: caller-provided clock + acquire_wait() + caller's wait/poll loop
    #[cfg(feature = "std")]
    pub fn request_abortable(&self, bytes: u64, should_stop: impl Fn() -> bool) -> bool {
        self.request(bytes, should_stop, true)
    }

    /// The wait behind both request wrappers; `withdraw_on_stop` returns the
    /// debit when the caller stops.
    #[cfg(feature = "std")]
    fn request(&self, bytes: u64, should_stop: impl Fn() -> bool, withdraw_on_stop: bool) -> bool {
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
        let Some(ticket) = self.take_ticket(bytes, Self::std_now()) else {
            return false;
        };
        loop {
            if should_stop() {
                if withdraw_on_stop {
                    self.withdraw(&ticket, bytes);
                }
                return true;
            }
            let Some(wait) = self.remaining(&ticket, Self::std_now()) else {
                return false;
            };
            std::thread::sleep(wait.min(Self::POLL_INTERVAL));
        }
    }

    /// Debits `bytes` and returns its ticket; `None` when the rate is `0`.
    #[cfg(feature = "std")]
    fn take_ticket(&self, bytes: u64, now: Duration) -> Option<Ticket> {
        let mut bucket = self.bucket.lock();
        let rate = self.rate();
        if rate == 0 {
            return None;
        }
        bucket.refill(rate, now.as_nanos());
        Some(Ticket {
            position: bucket.debit(bytes),
            taken_at: bucket.changes,
        })
    }

    /// What `ticket` still waits as of `now`, or `None` once it is released:
    /// repaid, or its bucket switched off since it was taken.
    #[cfg(feature = "std")]
    fn remaining(&self, ticket: &Ticket, now: Duration) -> Option<Duration> {
        let mut bucket = self.bucket.lock();
        if bucket.last_off > ticket.taken_at {
            return None;
        }
        // No switch off since the ticket, and the rate was nonzero when it
        // was taken, so it is nonzero now.
        let rate = self.rate();
        debug_assert_ne!(rate, 0);
        bucket.refill(rate, now.as_nanos());
        let wait = bucket.wait_for(ticket.position, rate);
        (!wait.is_zero()).then_some(wait)
    }

    /// Takes the debit of `ticket` out of line: the waiters behind it move up
    /// by `bytes`, the ones ahead of it are unaffected. Nothing is withdrawn
    /// once the rate was switched off since: that bucket, and the debit with
    /// it, is already gone.
    #[cfg(feature = "std")]
    fn withdraw(&self, ticket: &Ticket, bytes: u64) {
        let mut bucket = self.bucket.lock();
        if bucket.last_off > ticket.taken_at {
            return;
        }
        bucket.withdrawn.push((ticket.position, u128::from(bytes)));
        bucket.cap(self.rate());
        bucket.settle_withdrawn();
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

    /// `no_std` variant of the abortable wait: never sleeps, honors the stop
    /// signal, as the `no_std` `request_interruptible` does.
    // no-std: wire a caller-provided clock + `acquire_wait` poll loop to restore
    // throttling.
    #[cfg(not(feature = "std"))]
    pub fn request_abortable(&self, _bytes: u64, should_stop: impl Fn() -> bool) -> bool {
        should_stop()
    }

    /// Longest single sleep inside the request wrappers: the upper bound on
    /// how long a stop, a rate change or a withdrawn debit goes unnoticed.
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
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;
