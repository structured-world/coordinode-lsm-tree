// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Computed write-backpressure verdict.
//!
//! The engine is a library: `insert` is synchronous and non-blocking, and flush
//! plus compaction are driven by the caller, not by an engine-owned thread. So
//! backpressure is a **verdict the caller consults**, not an internal stall: the
//! engine would deadlock if it blocked a write on compaction debt draining, since
//! the blocked thread may be the one that runs compaction. This mirrors the
//! storage-admission predicate ([`crate::AbstractTree::write_admission`]).
//!
//! [`Backpressure`] is computed from three independent signals against
//! caller-configured thresholds (see the `*_slowdown` / `*_stop` fields on
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig)):
//!
//! - **L0 run count**: the signal the leveled trigger counts; each run costs a
//!   read one table lookup, so a tall L0 is what spikes read amplification.
//! - **Pending compaction bytes**: the size-target debt the strategy reports.
//! - **Unflushed memtable bytes**: the active plus the sealed memtables, the
//!   memory flushes have not yet released; for trees sharing a
//!   [`WriteBufferBudget`], their total.
//!
//! The verdict is the most severe of the three axes. With every threshold unset
//! the verdict is always [`Backpressure::None`], so the feature is off by default
//! and the write path is unchanged.
//!
//! Under [`Backpressure::Slowdown`] a tree with a write rate limit (see
//! [`Config::write_rate_limit`](crate::Config::write_rate_limit)) paces by bytes:
//! [`AbstractTree::write_backpressure_for`](crate::AbstractTree::write_backpressure_for)
//! debits a write's bytes from the tree's write limiter and returns what they
//! owe, so throughput under slowdown is bounded by the rate whatever the write
//! sizes.

use core::time::Duration;

/// Thresholds that drive the [`Backpressure`] verdict. Every field is opt-in:
/// `None` disables that axis. With all fields `None` the verdict is always
/// [`Backpressure::None`].
///
/// A `slowdown` threshold without its matching `stop` (or vice versa) is honoured
/// independently: the axis still produces the tier whose threshold is set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackpressureThresholds {
    /// L0 run count at or above which the verdict is at least
    /// [`Backpressure::Slowdown`].
    pub l0_slowdown: Option<usize>,
    /// L0 run count at or above which the verdict is [`Backpressure::Stop`].
    pub l0_stop: Option<usize>,
    /// Pending-compaction bytes at or above which the verdict is at least
    /// [`Backpressure::Slowdown`].
    pub bytes_slowdown: Option<u64>,
    /// Pending-compaction bytes at or above which the verdict is
    /// [`Backpressure::Stop`].
    pub bytes_stop: Option<u64>,
    /// Unflushed memtable bytes at or above which the verdict is at least
    /// [`Backpressure::Slowdown`].
    pub memtable_slowdown: Option<u64>,
    /// Unflushed memtable bytes at or above which the verdict is
    /// [`Backpressure::Stop`].
    pub memtable_stop: Option<u64>,
    /// The slowdown delay returned at the stop threshold. The actual
    /// [`Backpressure::Slowdown`] delay ramps linearly from zero at the slowdown
    /// threshold to this cap at the stop threshold, so there is no cliff between
    /// healthy and stopped. `None` (or `Duration::ZERO`) makes a slowdown tier
    /// report a zero-length delay (advisory tier only).
    pub max_slowdown: Option<Duration>,
}

impl BackpressureThresholds {
    /// All axes disabled: the verdict is always [`Backpressure::None`]. The
    /// default, and a usable `const` for config construction.
    pub const OFF: Self = Self {
        l0_slowdown: None,
        l0_stop: None,
        bytes_slowdown: None,
        bytes_stop: None,
        memtable_slowdown: None,
        memtable_stop: None,
        max_slowdown: None,
    };

    /// `true` when no axis is configured, so the verdict is always
    /// [`Backpressure::None`]. The hot path checks this first to skip the
    /// version inspection entirely.
    #[must_use]
    pub const fn is_off(&self) -> bool {
        self.l0_slowdown.is_none()
            && self.l0_stop.is_none()
            && self.bytes_slowdown.is_none()
            && self.bytes_stop.is_none()
            && self.memtable_slowdown.is_none()
            && self.memtable_stop.is_none()
    }
}

/// The live signals a [`Backpressure`] verdict is computed from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackpressureSignals {
    /// Runs in L0.
    pub l0_runs: usize,
    /// The size-target debt the compaction strategy reports.
    pub pending_compaction_bytes: u64,
    /// Bytes held by the active and the sealed memtables: the tree's own, or
    /// the total of every tree sharing its [`WriteBufferBudget`].
    pub unflushed_memtable_bytes: u64,
}

/// A computed write-backpressure verdict.
///
/// Advisory: the caller honours it in its own write loop (sleep at
/// [`Slowdown`](Backpressure::Slowdown), pause / shed at
/// [`Stop`](Backpressure::Stop)). The engine never blocks on it.
///
/// # Example
///
/// The caller consults the verdict before a write and honours it. Tracking how
/// long it spent throttled is the caller's own metric (the engine cannot observe
/// caller-side sleep time):
///
/// ```
/// use core::time::Duration;
/// use lsm_tree::Backpressure;
///
/// /// Returns how long the caller should pause before this write (zero = proceed).
/// fn honour(verdict: Backpressure) -> Duration {
///     match verdict {
///         Backpressure::None => Duration::ZERO,
///         Backpressure::Slowdown { suggested_delay } => suggested_delay,
///         // Stop: pause / shed load until a later verdict drops below stop.
///         Backpressure::Stop => Duration::from_millis(50),
///     }
/// }
///
/// assert_eq!(honour(Backpressure::None), Duration::ZERO);
/// assert_eq!(
///     honour(Backpressure::Slowdown { suggested_delay: Duration::from_micros(200) }),
///     Duration::from_micros(200),
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backpressure {
    /// The tree is within its target shape; write at full rate.
    None,
    /// The tree is past a slowdown threshold but below stop. The caller should
    /// delay each write by `suggested_delay` to let compaction catch up. The
    /// delay grows with the overage (zero at the slowdown threshold, up to the
    /// configured cap at the stop threshold).
    Slowdown {
        /// The recommended per-write delay.
        suggested_delay: Duration,
    },
    /// The tree is at or past a stop threshold. The caller should stop admitting
    /// writes until a later verdict drops back below stop (the verdict is
    /// computed, not latched, so it clears as soon as compaction drains).
    Stop,
}

impl Backpressure {
    /// Compute the verdict from the live `signals` against `thresholds`.
    ///
    /// Pure and allocation-free so it is unit-testable without a tree. The result
    /// is the most severe of the L0-run, pending-bytes and memtable-bytes axes;
    /// the slowdown delay is the largest of their ramped delays.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::{Backpressure, BackpressureSignals, BackpressureThresholds};
    ///
    /// let thresholds = BackpressureThresholds {
    ///     memtable_slowdown: Some(64 << 20),
    ///     memtable_stop: Some(256 << 20),
    ///     ..BackpressureThresholds::OFF
    /// };
    /// let signals = BackpressureSignals {
    ///     unflushed_memtable_bytes: 300 << 20,
    ///     ..BackpressureSignals::default()
    /// };
    /// assert_eq!(Backpressure::compute(&signals, &thresholds), Backpressure::Stop);
    /// ```
    #[must_use]
    pub fn compute(signals: &BackpressureSignals, thresholds: &BackpressureThresholds) -> Self {
        if thresholds.is_off() {
            return Self::None;
        }
        let BackpressureSignals {
            l0_runs,
            pending_compaction_bytes: pending,
            unflushed_memtable_bytes: unflushed,
        } = *signals;

        // Stop dominates: if any axis is at its stop threshold, stop.
        if thresholds.l0_stop.is_some_and(|t| l0_runs >= t)
            || thresholds.bytes_stop.is_some_and(|t| pending >= t)
            || thresholds.memtable_stop.is_some_and(|t| unflushed >= t)
        {
            return Self::Stop;
        }

        // Otherwise, the slowdown tier if any axis is past its slowdown
        // threshold. The delay is the largest ramp across the axes.
        let cap = thresholds.max_slowdown.unwrap_or(Duration::ZERO);
        let mut delay = Duration::ZERO;
        let mut slowing = false;

        if let Some(soft) = thresholds.l0_slowdown
            && l0_runs >= soft
        {
            slowing = true;
            delay = delay.max(ramp_usize(l0_runs, soft, thresholds.l0_stop, cap));
        }
        if let Some(soft) = thresholds.bytes_slowdown
            && pending >= soft
        {
            slowing = true;
            delay = delay.max(ramp_u64(pending, soft, thresholds.bytes_stop, cap));
        }
        if let Some(soft) = thresholds.memtable_slowdown
            && unflushed >= soft
        {
            slowing = true;
            delay = delay.max(ramp_u64(unflushed, soft, thresholds.memtable_stop, cap));
        }

        if slowing {
            Self::Slowdown {
                suggested_delay: delay,
            }
        } else {
            Self::None
        }
    }

    /// `true` for any tier above [`Backpressure::None`].
    #[must_use]
    pub const fn is_throttled(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// The current memtables of a tree, as a budget reads them: the tree's
/// lock-free mirror of its latest super version.
#[cfg(feature = "std")]
type MemtableProbe = alloc::sync::Weak<arc_swap::ArcSwap<crate::version::SuperVersion>>;

/// A write-buffer budget several trees share: the memtable axis of each
/// member's [`Backpressure`] verdict compares the members' unflushed memtable
/// bytes together, so N trees are bounded as one.
///
/// Hand the same `Arc` to every tree through
/// [`Config::write_buffer_budget`](crate::Config::write_buffer_budget). The
/// total is summed when a verdict asks for it, from each member's current
/// memtables, so a write pays nothing for the budget; a dropped tree leaves it.
///
/// # Examples
///
/// ```
/// use lsm_tree::{AbstractTree, Config, WriteBufferBudget};
/// use std::sync::Arc;
///
/// # let a = tempfile::tempdir()?;
/// # let b = tempfile::tempdir()?;
/// let budget = Arc::new(WriteBufferBudget::default());
/// let first = Config::new(a.path(), Default::default(), Default::default())
///     .write_buffer_budget(Arc::clone(&budget))
///     .open()?;
/// let second = Config::new(b.path(), Default::default(), Default::default())
///     .write_buffer_budget(Arc::clone(&budget))
///     .open()?;
/// first.insert("k", "v", 0);
/// second.insert("k", "v", 0);
/// assert!(budget.unflushed_bytes() >= 4);
/// # Ok::<(), lsm_tree::Error>(())
/// ```
// no-std: members would register their version history behind spin::RwLock
#[cfg(feature = "std")]
#[derive(Default)]
pub struct WriteBufferBudget {
    members: parking_lot::Mutex<alloc::vec::Vec<MemtableProbe>>,
}

#[cfg(feature = "std")]
impl WriteBufferBudget {
    /// Unflushed memtable bytes of every live member: their active and sealed
    /// memtables together.
    #[must_use]
    pub fn unflushed_bytes(&self) -> u64 {
        let mut members = self.members.lock();
        let mut total = 0u64;
        members.retain(|member| {
            let Some(mirror) = member.upgrade() else {
                return false;
            };
            // Each term is one tree's memtable bytes in memory, so the sum is
            // bounded by the process's memory and cannot overflow u64.
            total += mirror.load().unflushed_bytes();
            true
        });
        total
    }

    /// Adds a tree, by the mirror of its latest super version.
    pub(crate) fn register(&self, mirror: MemtableProbe) {
        self.members.lock().push(mirror);
    }
}

#[cfg(feature = "std")]
impl core::fmt::Debug for WriteBufferBudget {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteBufferBudget")
            .field("members", &self.members.lock().len())
            .finish()
    }
}

/// Linear ramp of `value` in `[soft, hard)` onto `[0, cap]`. Without a `hard`
/// bound the delay sits at `cap` once `value >= soft` (no interval to ramp over).
fn ramp_usize(value: usize, soft: usize, hard: Option<usize>, cap: Duration) -> Duration {
    match hard {
        // hard > soft: linear fraction of the cap. `value` is in `[soft, hard)`
        // here (stop tier was already handled), so `0 <= num < den`.
        Some(hard) if hard > soft => {
            let num = (value - soft) as u64;
            let den = (hard - soft) as u64;
            scale(cap, num, den)
        }
        _ => cap,
    }
}

/// `u64` twin of [`ramp_usize`].
fn ramp_u64(value: u64, soft: u64, hard: Option<u64>, cap: Duration) -> Duration {
    match hard {
        Some(hard) if hard > soft => {
            let num = value - soft;
            let den = hard - soft;
            scale(cap, num, den)
        }
        _ => cap,
    }
}

/// `cap * num / den` in nanoseconds, exact, with `num < den`.
fn scale(cap: Duration, num: u64, den: u64) -> Duration {
    if den == 0 {
        return cap;
    }
    // Apply the fraction WITHOUT forming `cap_nanos * num` first: for a
    // near-`Duration::MAX` cap and a wide span that product can exceed u128, and
    // saturating it would distort the ramp (collapsing it far below the intended
    // proportion). This mulDiv decomposition is exact and overflow-free given the
    // `num < den` invariant: the quotient term `(cap_nanos / den) * num` is
    // <= cap_nanos (since num <= den), and the remainder term's product
    // `(cap_nanos % den) * num` is < den * num < u128::MAX (both factors < u64::MAX).
    let cap_nanos = cap.as_nanos();
    let num = u128::from(num);
    let den = u128::from(den);
    let nanos = (cap_nanos / den) * num + ((cap_nanos % den) * num) / den;
    // secs <= cap.as_secs() (which is a u64), so try_from never actually
    // saturates here; the fallback is defensive only.
    let secs = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
    // n % 1_000_000_000 is < 1e9 < u32::MAX, so the subsec nanos fit u32.
    let sub = (nanos % 1_000_000_000) as u32;
    Duration::new(secs, sub)
}

#[cfg(test)]
mod tests;
