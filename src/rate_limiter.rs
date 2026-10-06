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
//! sleep until a deadline or an event that moves it) are gated behind the
//! `std` feature.
//!
//! # Sharing and retuning
//!
//! A tree builds its own limiter from
//! [`Config::compaction_rate_limit`](crate::Config::compaction_rate_limit),
//! or several trees share one handed to them through
//! [`Config::compaction_rate_limiter`](crate::Config::compaction_rate_limiter)
//! and are bounded by it together. The limiter owns the rate:
//! [`RateLimiter::set_rate`] changes it for every holder, with no restart.
//!
//! # Backing off while the device is slow
//!
//! A byte rate alone keeps background I/O going at that rate while other load
//! congests the device. With a [`LatencyBackoff`] set, the limiter also
//! watches how long its own charged reads take (fed in through
//! [`RateLimiter::record_read_latency`]) and lowers the rate it actually
//! grants while their smoothed latency is above a ceiling, down to a floor
//! that is never zero, then raises it back once the latency falls below the
//! ceiling's hysteresis band. Load from anything else on the device slows
//! these reads too, so foreign congestion shows in them with no device
//! mapping; congestion the limiter's own I/O causes falls as it backs off.
//!
//! Every reader the limiter paces reports: a paced verification scan, and a
//! compaction on the limiter, for the reads of its input tables and of the
//! blob files it relocates. A limiter shared by both thus keeps getting samples
//! once a scan ends, and climbs back as soon as the device answers fast again.
//! A compaction times its reads only while a backoff is set, a portion at a
//! time, so without one its reads go as they would unpaced.
//!
//! The law is the one PARDA uses to size a host's I/O window (Gulati, Ahmad,
//! Waldspurger, FAST 2009, section 3.2): the latency is an EWMA,
//! `L = (1 - α)·l + α·L'`, and each step moves the rate fraction `w` by
//! `w ← w·((1 - γ) + γ·ceiling / L)`, bounded by the floor and the configured
//! rate.

use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
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
/// the credit accrued reaches that position. A waiter re-reads the totals each
/// time it wakes instead of keeping its own countdown, and everything that
/// moves the line under it (a rate change, a debit withdrawn ahead of it)
/// wakes it. Totals are in bytes of `u64` requests summed in `u128`,
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
    /// The latency controller, when one is set.
    backoff: Option<Backoff>,
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
            backoff: None,
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

/// How a [`RateLimiter`] backs off while its reads are slow.
///
/// It holds the latency ceiling, the band below it the latency must fall to
/// before the rate climbs back, the lowest share of the configured rate it
/// goes down to, and how often the rate may step.
///
/// Set on a limiter with [`RateLimiter::set_latency_backoff`]; it can be
/// replaced or removed on a live limiter.
///
/// # Examples
///
/// ```
/// use lsm_tree::rate_limiter::LatencyBackoff;
/// use std::time::Duration;
///
/// let backoff = LatencyBackoff::new(Duration::from_millis(10))
///     .with_hysteresis(0.3)
///     .with_floor(0.1);
/// assert_eq!(backoff.ceiling(), Duration::from_millis(10));
/// assert_eq!(backoff.floor(), 0.1);
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatencyBackoff {
    ceiling: Duration,
    hysteresis: f64,
    floor: f64,
    period: Duration,
}

impl LatencyBackoff {
    /// Default [`hysteresis`](Self::with_hysteresis): the rate climbs back
    /// once the latency is 20% under the ceiling.
    pub const DEFAULT_HYSTERESIS: f64 = 0.2;

    /// Default [`floor`](Self::with_floor): 5% of the configured rate, the
    /// lowest share `RocksDB`'s auto-tuned limiter goes down to.
    pub const DEFAULT_FLOOR: f64 = 0.05;

    /// Default [`period`](Self::with_period) between two rate steps.
    pub const DEFAULT_PERIOD: Duration = Duration::from_millis(100);

    /// Backs off while the smoothed read latency is above `ceiling`, with
    /// the default hysteresis, floor and period.
    #[must_use]
    pub const fn new(ceiling: Duration) -> Self {
        Self {
            ceiling,
            hysteresis: Self::DEFAULT_HYSTERESIS,
            floor: Self::DEFAULT_FLOOR,
            period: Self::DEFAULT_PERIOD,
        }
    }

    /// Sets the band, as a fraction of the ceiling, the latency must fall
    /// below the ceiling before the rate climbs back: with `0.2` and a 10 ms
    /// ceiling, the rate holds between 8 and 10 ms and climbs under 8 ms.
    ///
    /// Clamped to `[0, 0.95]`; a value that is not a number keeps the default.
    #[must_use]
    pub fn with_hysteresis(mut self, fraction: f64) -> Self {
        self.hysteresis = if fraction.is_nan() {
            Self::DEFAULT_HYSTERESIS
        } else {
            fraction.clamp(0.0, 0.95)
        };
        self
    }

    /// Sets the lowest share of the configured rate the limiter backs off to.
    /// It is never zero, so a periodic scrub keeps moving however slow the
    /// device gets and keeps sampling the latency it recovers on.
    ///
    /// Clamped to `[0.001, 1]`; a value that is not a number keeps the
    /// default. `1` disables the backoff in effect.
    #[must_use]
    pub fn with_floor(mut self, fraction: f64) -> Self {
        self.floor = if fraction.is_nan() {
            Self::DEFAULT_FLOOR
        } else {
            fraction.clamp(0.001, 1.0)
        };
        self
    }

    /// Sets the shortest time between two rate steps, so a burst of reads
    /// moves the rate once per period rather than once per read.
    #[must_use]
    pub const fn with_period(mut self, period: Duration) -> Self {
        self.period = period;
        self
    }

    /// The smoothed read latency above which the rate steps down.
    #[must_use]
    pub const fn ceiling(&self) -> Duration {
        self.ceiling
    }

    /// The hysteresis band, as a fraction of the ceiling.
    #[must_use]
    pub const fn hysteresis(&self) -> f64 {
        self.hysteresis
    }

    /// The lowest share of the configured rate the limiter backs off to.
    #[must_use]
    pub const fn floor(&self) -> f64 {
        self.floor
    }

    /// The shortest time between two rate steps.
    #[must_use]
    pub const fn period(&self) -> Duration {
        self.period
    }
}

/// Weight of the history in the latency EWMA (PARDA's `α`): 7/8, as TCP
/// smooths its round-trip time, so one slow read moves the estimate by an
/// eighth of the difference.
const LATENCY_SMOOTHING: f64 = 0.875;

/// How far one step moves the rate toward where the ceiling would put it
/// (PARDA's `γ`): halfway.
const STEP_GAIN: f64 = 0.5;

/// The most one step may raise the rate fraction by, so a burst of fast reads
/// after a congested spell climbs back over a few periods instead of at once.
const MAX_STEP_UP: f64 = 2.0;

/// The latency controller's state.
#[derive(Debug)]
struct Backoff {
    config: LatencyBackoff,
    /// Smoothed read latency in nanoseconds; `None` before the first sample.
    smoothed_nanos: Option<f64>,
    /// The share of the configured rate granted, in `[floor, 1]`.
    fraction: f64,
    /// When the rate last stepped, as nanoseconds since the limiter's origin.
    last_step_nanos: u128,
}

impl Backoff {
    fn new(config: LatencyBackoff, now_nanos: u128) -> Self {
        Self {
            config,
            smoothed_nanos: None,
            fraction: 1.0,
            last_step_nanos: now_nanos,
        }
    }

    /// Folds a read latency into the estimate and, once a period has passed
    /// since the last step, steps the rate fraction.
    #[expect(
        clippy::cast_precision_loss,
        reason = "latencies in nanoseconds as f64 lose precision only past 2^53 ns, about 104 days"
    )]
    #[expect(
        clippy::suboptimal_flops,
        reason = "f64::mul_add is not in core, and this module builds without std"
    )]
    fn observe(&mut self, latency: Duration, now_nanos: u128) {
        let sample = latency.as_nanos() as f64;
        let smoothed = match self.smoothed_nanos {
            None => sample,
            Some(previous) => (1.0 - LATENCY_SMOOTHING) * sample + LATENCY_SMOOTHING * previous,
        };
        self.smoothed_nanos = Some(smoothed);

        if now_nanos.saturating_sub(self.last_step_nanos) < self.config.period.as_nanos() {
            return;
        }
        self.last_step_nanos = now_nanos;

        let ceiling = self.config.ceiling.as_nanos() as f64;
        let climbs_below = ceiling * (1.0 - self.config.hysteresis);
        if smoothed > ceiling {
            // `ceiling / smoothed < 1`: the fraction shrinks.
            let step = (1.0 - STEP_GAIN) + STEP_GAIN * (ceiling / smoothed);
            self.fraction = (self.fraction * step).max(self.config.floor);
        } else if smoothed < climbs_below {
            // `ceiling / smoothed > 1` (infinite for a zero latency, which the
            // step bound absorbs): the fraction grows.
            let step = ((1.0 - STEP_GAIN) + STEP_GAIN * (ceiling / smoothed)).min(MAX_STEP_UP);
            self.fraction = (self.fraction * step).min(1.0);
        }
    }

    /// Replaces the configuration, keeping the estimate and the fraction,
    /// lifted to the new floor.
    fn reconfigure(&mut self, config: LatencyBackoff) {
        self.config = config;
        self.fraction = self.fraction.max(config.floor);
    }
}

/// The rate actually granted: the configured rate scaled by the backoff, at
/// least one byte per second while the configured rate is on.
fn effective_rate(configured: u64, backoff: Option<&Backoff>) -> u64 {
    match backoff {
        Some(backoff) if configured != 0 && backoff.fraction < 1.0 => {
            #[expect(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "the product is in [0, configured], a fraction of a u64"
            )]
            let scaled = (configured as f64 * backoff.fraction) as u64;
            scaled.clamp(1, configured)
        }
        _ => configured,
    }
}

/// A debit waiting to be repaid: its place in line and the change count it
/// was taken at.
#[cfg(feature = "std")]
struct Ticket {
    position: u128,
    taken_at: u64,
}

/// What a waiting request sleeps on: rung by every event that can move it
/// (a rate change, a debit withdrawn ahead of it, a stop of the tree it works
/// for), so a wait nothing disturbs wakes once, at its own deadline.
///
/// A waiter reads the generation before it reads anything the event changes,
/// and sleeps only while the generation is still that one: an event between
/// the two is never slept through.
#[cfg(feature = "std")]
#[derive(Debug, Default)]
pub(crate) struct Wakeup {
    generation: parking_lot::Mutex<u64>,
    rung: parking_lot::Condvar,
}

#[cfg(feature = "std")]
impl Wakeup {
    /// The events rung so far.
    fn generation(&self) -> u64 {
        *self.generation.lock()
    }

    /// Wakes every waiter, to look again at what it waits for.
    pub(crate) fn ring(&self) {
        // One step per event: a u64 does not wrap. A waiter checks the
        // generation under the lock and sleeps without releasing it in
        // between, so the bump is seen whether it lands before or during
        // the sleep.
        *self.generation.lock() += 1;
        self.rung.notify_all();
    }

    /// Sleeps for `timeout`, or until an event after `seen` is rung.
    fn sleep(&self, seen: u64, timeout: Duration) {
        let mut generation = self.generation.lock();
        if *generation == seen {
            // Woken early or late, the caller re-derives its wait either way.
            // A timeout past the clock's range does not panic: parking_lot
            // takes a deadline that `Instant` cannot hold as no deadline, and
            // a ring still ends that wait.
            let _ = self.rung.wait_for(&mut generation, timeout);
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
    /// The rate set through [`set_rate`](Self::set_rate), in bytes per
    /// second. `0` means unlimited (disabled).
    configured_bytes_per_sec: AtomicU64,
    /// The refill rate actually granted: the configured rate, lowered by the
    /// latency backoff when one is set. `0` exactly when the configured rate
    /// is. Written only under the bucket lock, so a request that holds the
    /// lock sees the rate its bucket state was settled against.
    rate_bytes_per_sec: AtomicU64,
    /// Whether a latency backoff is set, so a reader can skip timing its
    /// reads without taking the bucket lock. Written under the bucket lock.
    steers_by_latency: AtomicBool,
    bucket: Mutex<Bucket>,
    /// What waiting requests sleep on; shared with the stop signals of the
    /// trees that hold this limiter.
    // no-std: the no_std requests never wait
    #[cfg(feature = "std")]
    wakeup: alloc::sync::Arc<Wakeup>,
}

impl RateLimiter {
    /// Creates a limiter refilling at `rate_bytes_per_sec`.
    ///
    /// `0` disables throttling (every request is immediate). The burst
    /// ceiling is one second of rate.
    #[must_use]
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        Self {
            configured_bytes_per_sec: AtomicU64::new(rate_bytes_per_sec),
            rate_bytes_per_sec: AtomicU64::new(rate_bytes_per_sec),
            steers_by_latency: AtomicBool::new(false),
            bucket: Mutex::new(Bucket::full(rate_bytes_per_sec, 0)),
            #[cfg(feature = "std")]
            wakeup: alloc::sync::Arc::default(),
        }
    }

    /// What this limiter's waiters sleep on, for a stop signal to ring.
    #[cfg(feature = "std")]
    pub(crate) fn wakeup(&self) -> alloc::sync::Arc<Wakeup> {
        alloc::sync::Arc::clone(&self.wakeup)
    }

    /// Wakes every request waiting on this limiter, to check its stop
    /// condition at once.
    ///
    /// A waiting request sleeps until its deadline or until something wakes
    /// it: a rate change and a returned debit do on their own, and so does a
    /// stop sent through the stop signal of a tree holding this limiter. A
    /// caller that stops its requests through some other flag calls this
    /// after setting it.
    // no-std: the no_std requests never wait
    #[cfg(feature = "std")]
    pub fn wake_waiters(&self) {
        self.wakeup.ring();
    }

    /// The configured rate in bytes per second; `0` when throttling is off.
    #[must_use]
    pub fn rate(&self) -> u64 {
        self.configured_bytes_per_sec.load(Ordering::Relaxed)
    }

    /// The rate granted now: the configured rate, or less while a
    /// [`LatencyBackoff`] holds it down.
    #[must_use]
    pub fn effective_rate(&self) -> u64 {
        self.rate_bytes_per_sec.load(Ordering::Relaxed)
    }

    /// Moves the granted rate to `new` at `now_nanos`, settling the bucket as
    /// [`set_rate_at`](Self::set_rate_at) describes. Returns whether it moved,
    /// so the caller wakes the waiters once the lock is released.
    fn switch_rate(&self, bucket: &mut Bucket, new: u64, now_nanos: u128) -> bool {
        let old = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if old == new {
            return false;
        }
        if old == 0 {
            bucket.fill(new, now_nanos);
        } else {
            bucket.refill(old, now_nanos);
            // Time the old rate did not turn into a whole byte is dropped
            // with it: it is not owed at the new rate.
            bucket.advance_clock(now_nanos);
            if new != 0 {
                bucket.cap(new);
            }
        }
        // One step per rate change: a u64 does not wrap.
        bucket.changes += 1;
        if new == 0 {
            bucket.last_off = bucket.changes;
        }
        self.rate_bytes_per_sec.store(new, Ordering::Relaxed);
        true
    }

    /// Wakes the waiters after the granted rate moved: every deadline moved,
    /// or a wait is over.
    fn rate_moved(&self) {
        #[cfg(feature = "std")]
        self.wakeup.ring();
    }

    /// Sets, replaces or (with `None`) removes the latency backoff at
    /// monotonic time `now`, for every holder of this limiter.
    ///
    /// A replacement keeps the latency estimate and the share of the rate
    /// granted, lifted to the new floor; a removal grants the configured rate
    /// again at once. `now` is on the same clock as
    /// [`acquire_wait`](Self::acquire_wait); with the `std` feature,
    /// [`set_latency_backoff`](Self::set_latency_backoff) reads it.
    pub fn set_latency_backoff_at(&self, backoff: Option<LatencyBackoff>, now: Duration) {
        let now_nanos = now.as_nanos();
        let mut bucket = self.bucket.lock();
        match (backoff, bucket.backoff.as_mut()) {
            (None, _) => bucket.backoff = None,
            (Some(config), Some(current)) => current.reconfigure(config),
            (Some(config), None) => bucket.backoff = Some(Backoff::new(config, now_nanos)),
        }
        self.steers_by_latency
            .store(bucket.backoff.is_some(), Ordering::Relaxed);
        let granted = effective_rate(self.rate(), bucket.backoff.as_ref());
        let moved = self.switch_rate(&mut bucket, granted, now_nanos);
        drop(bucket);
        if moved {
            self.rate_moved();
        }
    }

    /// Sets, replaces or removes the latency backoff now; see
    /// [`set_latency_backoff_at`](Self::set_latency_backoff_at).
    // no-std: set_latency_backoff_at with a caller-provided monotonic clock
    #[cfg(feature = "std")]
    pub fn set_latency_backoff(&self, backoff: Option<LatencyBackoff>) {
        self.set_latency_backoff_at(backoff, Self::std_now());
    }

    /// Whether reads charged to this limiter should report how long they take:
    /// a latency backoff is set and the rate is on. Lock-free, for a reader
    /// deciding per read whether to time it.
    #[must_use]
    pub(crate) fn steers_by_latency(&self) -> bool {
        self.steers_by_latency.load(Ordering::Relaxed)
            && self.rate_bytes_per_sec.load(Ordering::Relaxed) != 0
    }

    /// The latency backoff in force, if any.
    #[must_use]
    pub fn latency_backoff(&self) -> Option<LatencyBackoff> {
        self.bucket.lock().backoff.as_ref().map(|b| b.config)
    }

    /// Records how long one read this limiter charged took, observed at
    /// monotonic time `now`, and steps the granted rate if a period has
    /// passed since the last step. A no-op without a latency backoff.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::rate_limiter::{LatencyBackoff, RateLimiter};
    /// use std::time::Duration;
    ///
    /// let limiter = RateLimiter::new(1_000_000);
    /// let backoff = LatencyBackoff::new(Duration::from_millis(10)).with_period(Duration::ZERO);
    /// limiter.set_latency_backoff_at(Some(backoff), Duration::ZERO);
    ///
    /// // Reads four times slower than the ceiling: the granted rate drops.
    /// limiter.record_read_latency_at(Duration::from_millis(40), Duration::from_millis(1));
    /// assert!(limiter.effective_rate() < limiter.rate());
    /// ```
    pub fn record_read_latency_at(&self, latency: Duration, now: Duration) {
        if self.rate_bytes_per_sec.load(Ordering::Relaxed) == 0 {
            return;
        }
        let now_nanos = now.as_nanos();
        let mut bucket = self.bucket.lock();
        let Some(backoff) = bucket.backoff.as_mut() else {
            return;
        };
        backoff.observe(latency, now_nanos);
        let granted = effective_rate(self.rate(), bucket.backoff.as_ref());
        let moved = self.switch_rate(&mut bucket, granted, now_nanos);
        drop(bucket);
        if moved {
            self.rate_moved();
        }
    }

    /// Records how long one read this limiter charged took, as of now; see
    /// [`record_read_latency_at`](Self::record_read_latency_at).
    // no-std: record_read_latency_at with a caller-provided monotonic clock
    #[cfg(feature = "std")]
    pub fn record_read_latency(&self, latency: Duration) {
        self.record_read_latency_at(latency, Self::std_now());
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
    ///
    /// With a [`LatencyBackoff`] set, the granted rate is the new rate scaled
    /// by the share the backoff holds it to.
    pub fn set_rate_at(&self, bytes_per_sec: u64, now: Duration) {
        let now_nanos = now.as_nanos();
        let mut bucket = self.bucket.lock();
        self.configured_bytes_per_sec
            .store(bytes_per_sec, Ordering::Relaxed);
        let granted = effective_rate(bytes_per_sec, bucket.backoff.as_ref());
        let moved = self.switch_rate(&mut bucket, granted, now_nanos);
        drop(bucket);
        if moved {
            self.rate_moved();
        }
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
        let rate = self.effective_rate();
        if rate == 0 {
            return Duration::ZERO;
        }
        bucket.refill(rate, now.as_nanos());
        let position = bucket.debit(bytes);
        bucket.wait_for(position, rate)
    }

    /// Waits (sleeping the current thread) until an I/O of `bytes` may
    /// proceed, checking `should_stop` so a shutdown can break a long wait.
    /// Use it where the I/O may still happen after a stop; a caller that does
    /// no I/O once stopped uses
    /// [`request_abortable`](Self::request_abortable).
    ///
    /// Returns `true` if `should_stop` was set on entry or fired during the
    /// wait, `false` once the wait is over (the caller may proceed). A stop
    /// keeps the debit: the budget stays spent for the I/O the caller may
    /// still do.
    ///
    /// The wait sleeps until the debit is repaid, re-deriving it from the
    /// shared bucket whenever something wakes it: a rate change, a switch
    /// off, a debit withdrawn ahead of this one, or a stop (see
    /// [`wake_waiters`](Self::wake_waiters) for which stops ring it). A
    /// no-op returning `false` when the rate is `0` (no clock read). Only
    /// with the `std` feature; `no_std` callers drive `acquire_wait` with
    /// their own clock.
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
            // Read before the state it guards: an event from here on is not
            // slept through.
            let seen = self.wakeup.generation();
            if should_stop() {
                if withdraw_on_stop {
                    self.withdraw(&ticket, bytes);
                }
                return true;
            }
            let Some(wait) = self.remaining(&ticket, Self::std_now()) else {
                return false;
            };
            self.wakeup.sleep(seen, wait);
        }
    }

    /// Debits `bytes` and returns its ticket; `None` when the rate is `0`.
    #[cfg(feature = "std")]
    fn take_ticket(&self, bytes: u64, now: Duration) -> Option<Ticket> {
        let mut bucket = self.bucket.lock();
        let rate = self.effective_rate();
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
        let rate = self.effective_rate();
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
        bucket.cap(self.effective_rate());
        bucket.settle_withdrawn();
        drop(bucket);
        // The waiters behind it moved up the line.
        self.wakeup.ring();
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
