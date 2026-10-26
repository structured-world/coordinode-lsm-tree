// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A cumulative host-write budget for one physical medium.
//!
//! A [`RateLimiter`](crate::rate_limiter::RateLimiter) paces how fast
//! compaction writes; it does not bound how much a medium takes over a day or
//! a week, which is what a device with a rated endurance needs. A
//! [`WriteBudget`] does: it is a byte allowance per period, refilled as the
//! wall clock moves and capped at a burst, against which every byte written
//! through the medium is charged, and which discretionary work reserves before
//! it starts.
//!
//! # What is charged
//!
//! The budget reads the medium's write ledger, the [`WriteIoStats`] every
//! backend on that medium counts into: the bytes the files' writes accepted,
//! of every origin, and the length of every clone (an upper bound of what a
//! copying backend wrote below the ledger). Flushes, ingestion, the manifest
//! and repair are charged as they write, though nothing here holds them back:
//! the budget throttles only work that may wait. Compaction reserves its
//! expected output before it runs; the reservation is given back as its
//! output files land in the ledger, and wholly once the compaction ends.
//!
//! These are host writes, the bytes handed to the backend. What the device
//! then programs into its flash lies below them and is not known here.
//!
//! # Refill
//!
//! The allowance refills continuously, `bytes_per_period` every `period` of
//! wall time, and holds at most `burst` bytes of credit, so over any span `T`
//! it credits at most `burst + bytes_per_period * T / period`, the way a rated
//! endurance is stated per day. A reservation is granted only while the credit
//! left covers it. Writes nothing holds back, and output past what its work
//! reserved, can take the charge past the credit: the budget is then overrun,
//! reports it, and grants nothing until the refill has repaid it.
//!
//! The budget reads the ledger whenever it is asked anything and dates what it
//! finds then: the refill up to that moment is added first, the new bytes
//! charged after. A full budget therefore gains nothing for the time between a
//! write and the next reading, which makes it stricter than the bound, never
//! looser.
//!
//! # Restart
//!
//! The budget lives in memory. [`WriteBudget::checkpoint`] reads what it
//! holds for the caller to keep, and [`WriteBudget::resume`] starts a budget
//! from it, charged a caller-stated bound of what was written after the
//! checkpoint and never counted. A budget started with [`WriteBudget::new`]
//! begins with a full burst, which is a fresh medium's state, not a restart's.
//!
//! # Clock
//!
//! The refill reads wall time, which crosses restarts where a monotonic clock
//! does not. Every operation has an `_at` form taking the time, for a caller
//! with its own clock and for builds without `std`; the plain forms read the
//! budget's [`Clock`](crate::Clock). A clock that steps back refills nothing until it passes
//! the time already counted.

use crate::fs::{WriteIoSnapshot, WriteIoStats, WriteOrigin};
use alloc::sync::Arc;
use core::time::Duration;

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// How much a medium may be written per period.
///
/// # Examples
///
/// ```
/// use lsm_tree::write_budget::Allowance;
/// use std::time::Duration;
///
/// // 8 GiB a day, at most 2 GiB of it at once.
/// let allowance = Allowance::new(8 << 30, Duration::from_secs(86_400)).with_burst(2 << 30);
/// assert_eq!(allowance.burst(), 2 << 30);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allowance {
    bytes_per_period: u64,
    period: Duration,
    burst: u64,
}

impl Allowance {
    /// `bytes_per_period` every `period`, with a burst of one period's bytes.
    ///
    /// A zero period is taken as one nanosecond, which refills the burst at
    /// once: a budget that limits nothing.
    #[must_use]
    pub const fn new(bytes_per_period: u64, period: Duration) -> Self {
        Self {
            bytes_per_period,
            period,
            burst: bytes_per_period,
        }
    }

    /// Sets the most credit the budget holds: what idle time saves up for one
    /// burst of work.
    #[must_use]
    pub const fn with_burst(mut self, bytes: u64) -> Self {
        self.burst = bytes;
        self
    }

    /// Bytes refilled per [`Self::period`].
    #[must_use]
    pub const fn bytes_per_period(&self) -> u64 {
        self.bytes_per_period
    }

    /// The period [`Self::bytes_per_period`] refills over.
    #[must_use]
    pub const fn period(&self) -> Duration {
        self.period
    }

    /// The most credit the budget holds.
    #[must_use]
    pub const fn burst(&self) -> u64 {
        self.burst
    }

    fn period_nanos(&self) -> u128 {
        self.period.as_nanos().max(1)
    }
}

/// What a budget holds, for the caller to keep across a restart.
///
/// The credit at a moment of wall time, negative while the budget is overrun.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetCheckpoint {
    /// Bytes of credit; negative while what was charged exceeds the refill.
    pub credit: i64,
    /// The wall time, since the Unix epoch, the credit was refilled to.
    pub as_of: Duration,
}

/// Why a reservation was granted past the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Override {
    /// The tree's shape crossed a threshold that stops its writers: waiting
    /// for credit would stall them.
    Backpressure,
    /// The caller asked for the work to run, through
    /// [`WriteBudget::set_forced`]: a read latency it cannot keep, or space it
    /// must reclaim.
    Forced,
}

/// Reservations granted past the budget for one [`Override`] reason.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Overrides {
    /// Reservations granted.
    pub reservations: u64,
    /// Bytes they reserved.
    pub bytes: u64,
}

impl Overrides {
    fn add(&mut self, bytes: u64) {
        self.reservations += 1;
        self.bytes += bytes;
    }
}

/// A reading of a budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BudgetReport {
    /// Bytes of credit; negative while the budget is overrun.
    pub credit: i64,
    /// Bytes reserved by work in progress that its output has not yet
    /// charged.
    pub reserved: u64,
    /// Bytes charged that compaction wrote.
    pub discretionary_bytes: u64,
    /// Bytes charged that everything else wrote: flushes, ingestion, the
    /// manifest and metadata, repair, checkpoints.
    pub mandatory_bytes: u64,
    /// Reservations refused for want of credit.
    pub refused: u64,
    /// The wall time of the earliest refusal since a reservation was last
    /// granted: how long work has been waiting, `None` when nothing waits.
    pub waiting_since: Option<Duration>,
    /// Reservations granted past the budget because the tree's writers would
    /// stall ([`Override::Backpressure`]).
    pub backpressure_overrides: Overrides,
    /// Reservations granted past the budget because the caller asked
    /// ([`Override::Forced`]).
    pub forced_overrides: Overrides,
    /// Whether the writes the budget cannot hold back alone take more than
    /// the allowance refills: no throttle of compaction keeps such a load
    /// within the budget.
    pub unsustainable: bool,
}

/// The ledger's bytes, split by whether the budget may hold their work back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Spend {
    discretionary: u64,
    mandatory: u64,
}

impl Spend {
    fn of(snapshot: &WriteIoSnapshot) -> Self {
        let discretionary = snapshot.origin(WriteOrigin::CompactionTable).bytes
            + snapshot.origin(WriteOrigin::CompactionBlob).bytes;
        // Every file byte of every origin is in `files()`, the compaction's
        // among them; a clone's length is charged as written.
        let mandatory = snapshot.files().bytes - discretionary + snapshot.directory.cloned_bytes;
        Self {
            discretionary,
            mandatory,
        }
    }
}

/// Mandatory bytes over the current and the previous period, for a sliding
/// estimate of the last period's.
#[derive(Clone, Copy, Debug, Default)]
struct Window {
    /// The index of the current period, counted from the epoch.
    index: u128,
    current: u64,
    previous: u64,
}

impl Window {
    /// Moves the window to the period of `now_nanos`.
    fn advance(&mut self, now_nanos: u128, period_nanos: u128) {
        let index = now_nanos / period_nanos;
        if index <= self.index {
            return;
        }
        self.previous = if index == self.index + 1 {
            self.current
        } else {
            0
        };
        self.current = 0;
        self.index = index;
    }

    /// The bytes of the last period up to `now_nanos`: all of the current
    /// period's and the share of the previous one's that lies within it.
    fn last_period(&self, now_nanos: u128, period_nanos: u128) -> u128 {
        let into = now_nanos % period_nanos;
        u128::from(self.current) + u128::from(self.previous) * (period_nanos - into) / period_nanos
    }
}

#[derive(Debug)]
struct Account {
    allowance: Allowance,
    /// Bytes of credit, negative while overrun.
    credit: i128,
    /// Wall time, in nanoseconds since the epoch, the credit is refilled to.
    as_of: u128,
    /// The ledger as last charged.
    seen: Spend,
    reserved: u64,
    refused: u64,
    waiting_since: Option<u128>,
    backpressure_overrides: Overrides,
    forced_overrides: Overrides,
    window: Window,
}

impl Account {
    /// Adds what the allowance refilled since `as_of`, up to the burst. Only
    /// whole bytes are credited; the time of a partial byte carries over.
    fn refill(&mut self, now_nanos: u128) {
        let elapsed = now_nanos.saturating_sub(self.as_of);
        let rate = u128::from(self.allowance.bytes_per_period);
        let period = self.allowance.period_nanos();
        let burst = i128::from(self.allowance.burst);
        if elapsed == 0 {
            return;
        }
        if rate == 0 || self.credit >= burst {
            // Nothing to add, or nothing it could add: the time is spent.
            self.as_of = now_nanos;
            return;
        }
        // An absurd span saturates and is capped at the burst below.
        let refilled = elapsed.saturating_mul(rate) / period;
        // Positive: the credit is below the burst.
        let room = u128::try_from(burst - self.credit).unwrap_or(u128::MAX);
        if refilled >= room {
            self.credit = burst;
            self.as_of = now_nanos;
        } else {
            // Below `room`, which fits an i128 as a difference of two.
            self.credit += i128::try_from(refilled).unwrap_or(i128::MAX);
            // The time those bytes took, rounded up so none is paid twice;
            // never past `now_nanos`, since `refilled` was rounded down.
            self.as_of += (refilled * period).div_ceil(rate);
        }
    }

    /// Charges what the ledger counted since it was last read.
    fn charge(&mut self, now: Spend, now_nanos: u128) {
        // The ledger only grows, and the budget reads one set for its life.
        let discretionary = now.discretionary - self.seen.discretionary;
        let mandatory = now.mandatory - self.seen.mandatory;
        self.seen = now;
        self.credit -= i128::from(discretionary) + i128::from(mandatory);
        self.window
            .advance(now_nanos, self.allowance.period_nanos());
        self.window.current += mandatory;
    }

    fn settle(&mut self, now: Spend, now_nanos: u128) {
        self.refill(now_nanos);
        self.charge(now, now_nanos);
    }

    /// Credit left once what is reserved is set against it.
    fn free(&self) -> i128 {
        self.credit - i128::from(self.reserved)
    }

    fn unsustainable(&self, now_nanos: u128) -> bool {
        self.window
            .last_period(now_nanos, self.allowance.period_nanos())
            > u128::from(self.allowance.bytes_per_period)
    }
}

/// A cumulative host-write budget for one physical medium, shared by every
/// tree and level route written through it. See the [module docs](self).
///
/// # Examples
///
/// ```
/// use lsm_tree::fs::WriteIoStats;
/// use lsm_tree::write_budget::{Allowance, WriteBudget};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// let ledger = Arc::new(WriteIoStats::default());
/// let day = Duration::from_secs(86_400);
/// let budget = WriteBudget::new_at(Arc::clone(&ledger), Allowance::new(1_000, day), Duration::ZERO);
///
/// // Work that fits the credit is granted, and holds it while it runs.
/// let hold = budget.reserve_at(600, Duration::ZERO).expect("within the burst");
/// assert!(budget.reserve_at(600, Duration::ZERO).is_none());
/// // Given back unspent, the hold leaves the whole burst.
/// drop(hold);
/// assert!(budget.reserve_at(1_000, Duration::ZERO).is_some());
/// ```
pub struct WriteBudget {
    ledger: Arc<WriteIoStats>,
    account: Mutex<Account>,
    /// Whether the caller asks discretionary work to run past the budget.
    forced: portable_atomic::AtomicBool,
    #[cfg(feature = "std")]
    clock: Arc<dyn crate::Clock>,
}

impl core::fmt::Debug for WriteBudget {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteBudget")
            .field("account", &*self.account.lock())
            .finish_non_exhaustive()
    }
}

impl WriteBudget {
    /// A budget charging `ledger` against `allowance`, with a full burst of
    /// credit as of wall time `now`.
    #[must_use]
    pub fn new_at(ledger: Arc<WriteIoStats>, allowance: Allowance, now: Duration) -> Self {
        let credit = i128::from(allowance.burst);
        Self::start(ledger, allowance, credit, now)
    }

    /// A budget resumed from `checkpoint`, charging `ledger` against
    /// `allowance` from wall time `now`, less `unaccounted` bytes: the most
    /// that may have been written after the checkpoint without being
    /// charged. The refill between the checkpoint and `now` is credited, up
    /// to the burst, and nothing else.
    #[must_use]
    pub fn resume_at(
        ledger: Arc<WriteIoStats>,
        allowance: Allowance,
        checkpoint: BudgetCheckpoint,
        unaccounted: u64,
        now: Duration,
    ) -> Self {
        let budget = Self::start(
            ledger,
            allowance,
            i128::from(checkpoint.credit) - i128::from(unaccounted),
            checkpoint.as_of,
        );
        budget.account.lock().refill(now.as_nanos());
        budget
    }

    fn start(
        ledger: Arc<WriteIoStats>,
        allowance: Allowance,
        credit: i128,
        as_of: Duration,
    ) -> Self {
        // What the ledger counted before the budget is not the budget's.
        let seen = Spend::of(&ledger.snapshot());
        let as_of = as_of.as_nanos();
        let mut window = Window::default();
        window.advance(as_of, allowance.period_nanos());
        Self {
            ledger,
            account: Mutex::new(Account {
                allowance,
                credit: credit.min(i128::from(allowance.burst)),
                as_of,
                seen,
                reserved: 0,
                refused: 0,
                waiting_since: None,
                backpressure_overrides: Overrides::default(),
                forced_overrides: Overrides::default(),
                window,
            }),
            forced: portable_atomic::AtomicBool::new(false),
            #[cfg(feature = "std")]
            clock: Arc::new(crate::SystemClock),
        }
    }

    /// A budget charging `ledger` against `allowance`, with a full burst of
    /// credit as of now on the system clock.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn new(ledger: Arc<WriteIoStats>, allowance: Allowance) -> Self {
        Self::new_at(
            ledger,
            allowance,
            crate::Clock::unix_time(&crate::SystemClock),
        )
    }

    /// A budget resumed from `checkpoint` as of now on the system clock; see
    /// [`Self::resume_at`].
    #[cfg(feature = "std")]
    #[must_use]
    pub fn resume(
        ledger: Arc<WriteIoStats>,
        allowance: Allowance,
        checkpoint: BudgetCheckpoint,
        unaccounted: u64,
    ) -> Self {
        let now = crate::Clock::unix_time(&crate::SystemClock);
        Self::resume_at(ledger, allowance, checkpoint, unaccounted, now)
    }

    /// Reads wall time from `clock` instead of the system clock.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn crate::Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The write ledger the budget charges.
    #[must_use]
    pub fn ledger(&self) -> &Arc<WriteIoStats> {
        &self.ledger
    }

    #[cfg(feature = "std")]
    fn now(&self) -> Duration {
        self.clock.unix_time()
    }

    /// Reserves `bytes` of credit as of wall time `now` for work that may
    /// wait, or `None` when the credit left past what is reserved does not
    /// cover them.
    pub fn reserve_at(self: &Arc<Self>, bytes: u64, now: Duration) -> Option<BudgetHold> {
        let now_nanos = now.as_nanos();
        let mut account = self.account.lock();
        account.settle(Spend::of(&self.ledger.snapshot()), now_nanos);
        if account.free() < i128::from(bytes) {
            account.refused += 1;
            account.waiting_since.get_or_insert(now_nanos);
            return None;
        }
        let hold = self.take(&mut account, bytes);
        drop(account);
        Some(hold)
    }

    /// Reserves `bytes` as of wall time `now` whatever the credit, for work
    /// `reason` lets run past the budget. The work is charged as any other;
    /// the reservation is counted as an override.
    pub fn reserve_overriding_at(
        self: &Arc<Self>,
        bytes: u64,
        reason: Override,
        now: Duration,
    ) -> BudgetHold {
        let mut account = self.account.lock();
        account.settle(Spend::of(&self.ledger.snapshot()), now.as_nanos());
        match reason {
            Override::Backpressure => account.backpressure_overrides.add(bytes),
            Override::Forced => account.forced_overrides.add(bytes),
        }
        let hold = self.take(&mut account, bytes);
        drop(account);
        hold
    }

    fn take(self: &Arc<Self>, account: &mut Account, bytes: u64) -> BudgetHold {
        account.reserved += bytes;
        account.waiting_since = None;
        BudgetHold {
            budget: Arc::clone(self),
            held: Mutex::new(bytes),
        }
    }

    /// Reserves `bytes` as of now; see [`Self::reserve_at`].
    #[cfg(feature = "std")]
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Option<BudgetHold> {
        self.reserve_at(bytes, self.now())
    }

    /// Reserves `bytes` past the budget as of now; see
    /// [`Self::reserve_overriding_at`].
    #[cfg(feature = "std")]
    pub fn reserve_overriding(self: &Arc<Self>, bytes: u64, reason: Override) -> BudgetHold {
        self.reserve_overriding_at(bytes, reason, self.now())
    }

    /// Asks discretionary work to run whether or not the budget covers it,
    /// or (with `false`) to wait for credit again. For a caller whose reads
    /// slow past what it can serve, or that must reclaim space, while the
    /// budget holds compaction back: the work it lets through is charged and
    /// reported as an [`Override::Forced`].
    pub fn set_forced(&self, forced: bool) {
        self.forced
            .store(forced, portable_atomic::Ordering::Relaxed);
    }

    /// Whether discretionary work runs past the budget; see
    /// [`Self::set_forced`].
    #[must_use]
    pub fn forced(&self) -> bool {
        self.forced.load(portable_atomic::Ordering::Relaxed)
    }

    /// Changes the allowance as of wall time `now`: time up to `now` refills
    /// at the old one, and credit above the new burst is dropped.
    pub fn set_allowance_at(&self, allowance: Allowance, now: Duration) {
        let mut account = self.account.lock();
        account.settle(Spend::of(&self.ledger.snapshot()), now.as_nanos());
        account.allowance = allowance;
        account.credit = account.credit.min(i128::from(allowance.burst));
    }

    /// Changes the allowance as of now; see [`Self::set_allowance_at`].
    #[cfg(feature = "std")]
    pub fn set_allowance(&self, allowance: Allowance) {
        self.set_allowance_at(allowance, self.now());
    }

    /// The allowance in force.
    #[must_use]
    pub fn allowance(&self) -> Allowance {
        self.account.lock().allowance
    }

    /// What the budget holds as of wall time `now`, for the caller to keep
    /// and [resume](Self::resume_at) from.
    pub fn checkpoint_at(&self, now: Duration) -> BudgetCheckpoint {
        let mut account = self.account.lock();
        account.settle(Spend::of(&self.ledger.snapshot()), now.as_nanos());
        BudgetCheckpoint {
            credit: clamp_i64(account.credit),
            as_of: nanos_to_duration(account.as_of),
        }
    }

    /// What the budget holds as of now; see [`Self::checkpoint_at`].
    #[cfg(feature = "std")]
    pub fn checkpoint(&self) -> BudgetCheckpoint {
        self.checkpoint_at(self.now())
    }

    /// Reads the budget as of wall time `now`.
    pub fn report_at(&self, now: Duration) -> BudgetReport {
        let now_nanos = now.as_nanos();
        let mut account = self.account.lock();
        account.settle(Spend::of(&self.ledger.snapshot()), now_nanos);
        BudgetReport {
            credit: clamp_i64(account.credit),
            reserved: account.reserved,
            discretionary_bytes: account.seen.discretionary,
            mandatory_bytes: account.seen.mandatory,
            refused: account.refused,
            waiting_since: account.waiting_since.map(nanos_to_duration),
            backpressure_overrides: account.backpressure_overrides,
            forced_overrides: account.forced_overrides,
            unsustainable: account.unsustainable(now_nanos),
        }
    }

    /// Reads the budget as of now; see [`Self::report_at`].
    #[cfg(feature = "std")]
    pub fn report(&self) -> BudgetReport {
        self.report_at(self.now())
    }

    /// Gives back `bytes` of reservation.
    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut account = self.account.lock();
        debug_assert!(
            account.reserved >= bytes,
            "a hold gives back only what it took"
        );
        account.reserved -= bytes;
    }
}

/// Credit one piece of work holds while it runs, given back as its output
/// lands in the ledger and wholly once it is dropped.
///
/// Between a byte's write and its file's landing the byte is both charged and
/// held, so the budget reads tighter by at most the output files in flight.
pub struct BudgetHold {
    budget: Arc<WriteBudget>,
    held: Mutex<u64>,
}

impl core::fmt::Debug for BudgetHold {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BudgetHold")
            .field("held", &*self.held.lock())
            .finish_non_exhaustive()
    }
}

impl BudgetHold {
    /// A file of `len` bytes the work wrote landed in the ledger: that much
    /// of the hold is charged now and given back.
    pub fn landed(&self, len: u64) {
        let mut held = self.held.lock();
        let covered = len.min(*held);
        *held -= covered;
        drop(held);
        self.budget.release(covered);
    }

    /// The bytes still held.
    #[must_use]
    pub fn held(&self) -> u64 {
        *self.held.lock()
    }

    /// The budget the hold is on.
    #[must_use]
    pub fn budget(&self) -> &Arc<WriteBudget> {
        &self.budget
    }
}

impl Drop for BudgetHold {
    fn drop(&mut self) {
        let held = *self.held.get_mut();
        self.budget.release(held);
    }
}

fn clamp_i64(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

fn nanos_to_duration(nanos: u128) -> Duration {
    let secs = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let sub = (nanos % 1_000_000_000) as u32;
    Duration::new(secs, sub)
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
