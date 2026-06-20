// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use core::time::Duration;

/// A source of wall-clock time.
///
/// The engine reads wall-clock time (for the `created_at` stamp on tables and
/// blob files, and for FIFO TTL expiry) through this trait. Under `std` the
/// built-in [`SystemClock`] is used automatically. Under `no_std` there is no
/// ambient system clock, so a consumer (e.g. a WASM host exposing `Date.now()`)
/// injects one once via `set_clock` before opening a tree.
///
/// # Examples
///
/// ```
/// # use lsm_tree::Clock;
/// # use core::time::Duration;
/// struct FixedClock(Duration);
/// impl Clock for FixedClock {
///     fn unix_time(&self) -> Duration {
///         self.0
///     }
/// }
/// let clock = FixedClock(Duration::from_secs(1_700_000_000));
/// assert_eq!(clock.unix_time().as_secs(), 1_700_000_000);
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a wall-clock source",
    label = "this type does not implement `Clock`",
    note = "implement `Clock` to inject a wall-clock under `no_std`, or enable the `std` feature to use the built-in `SystemClock`"
)]
pub trait Clock: Send + Sync {
    /// Wall-clock time elapsed since the Unix epoch.
    ///
    /// A clock with no real time source should return [`Duration::ZERO`] (the
    /// epoch), which disables TTL expiry rather than expiring everything.
    fn unix_time(&self) -> Duration;
}

/// The system wall-clock, backed by [`std::time::SystemTime`].
///
/// The default [`Clock`] under `feature = "std"`; consumers never need to
/// register it explicitly.
#[cfg(feature = "std")]
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

#[cfg(feature = "std")]
impl Clock for SystemClock {
    fn unix_time(&self) -> Duration {
        #[expect(
            clippy::expect_used,
            reason = "the system clock predates the Unix epoch only on a grossly misconfigured host"
        )]
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("system time is before the Unix epoch")
    }
}

/// Gets the unix timestamp as a duration (wall-clock time since the epoch).
///
/// Reads through the active [`Clock`]: the built-in [`SystemClock`] under
/// `std`, or the caller-registered clock under `no_std` (see `set_clock`).
/// Until a `no_std` clock is registered the value is [`Duration::ZERO`]
/// (epoch), which disables TTL expiry rather than expiring everything.
pub fn unix_timestamp() -> Duration {
    #[cfg(test)]
    #[allow(clippy::significant_drop_in_scrutinee, clippy::expect_used)]
    {
        if let Some(cell) = NOW_OVERRIDE.get()
            && let Some(override_val) = *cell.lock().expect("lock is poisoned")
        {
            return override_val;
        }
    }

    #[cfg(feature = "std")]
    {
        SystemClock.unix_time()
    }

    #[cfg(not(feature = "std"))]
    {
        nostd_clock::now()
    }
}

/// Monotonic instant used for elapsed-time logging on the compaction / flush
/// paths.
///
/// Under `std` this is a re-export of [`std::time::Instant`]. Under `no_std`
/// there is no ambient monotonic clock, so this is a zero-sized stub whose
/// [`elapsed`](Instant::elapsed) always reports [`core::time::Duration::ZERO`]
/// — the timing logs degrade to `0ns` rather than failing to compile.
// no-std: wire a caller-provided monotonic Clock hook (mirroring the
// `unix_timestamp` wall-clock hook) if real elapsed timing is needed.
#[cfg(feature = "std")]
pub use std::time::Instant;

/// See the `std` variant — a no-op monotonic-instant stub for `no_std`.
#[cfg(not(feature = "std"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Instant;

#[cfg(not(feature = "std"))]
impl Instant {
    /// Returns the (stub) current instant.
    #[must_use]
    pub const fn now() -> Self {
        Self
    }

    /// Always [`core::time::Duration::ZERO`] under `no_std` (no monotonic clock).
    #[must_use]
    pub const fn elapsed(&self) -> core::time::Duration {
        core::time::Duration::ZERO
    }
}

/// Registers the [`Clock`] used by [`unix_timestamp`] under `no_std` (e.g. a
/// WASM host's `Date.now()`). Idempotent: the first registration wins, later
/// calls are ignored. Only present under `no_std`: under `std` the built-in
/// [`SystemClock`] is always available, so no registration is needed.
#[cfg(not(feature = "std"))]
pub fn set_clock(clock: alloc::boxed::Box<dyn Clock>) {
    nostd_clock::set(clock);
}

#[cfg(not(feature = "std"))]
mod nostd_clock {
    use super::Clock;
    use alloc::boxed::Box;
    use core::time::Duration;
    use once_cell::race::OnceBox;

    // Caller-injected wall-clock. Lock-free (atomic pointer), set once.
    static CLOCK: OnceBox<Box<dyn Clock>> = OnceBox::new();

    pub fn set(clock: Box<dyn Clock>) {
        let _ = CLOCK.set(Box::new(clock));
    }

    pub fn now() -> Duration {
        CLOCK
            .get()
            .map_or(Duration::ZERO, |clock| clock.unix_time())
    }
}

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
static NOW_OVERRIDE: OnceLock<Mutex<Option<std::time::Duration>>> = OnceLock::new();

#[cfg(test)]
#[allow(clippy::expect_used)]
pub fn set_unix_timestamp_for_test(value: Option<std::time::Duration>) {
    let cell = NOW_OVERRIDE.get_or_init(|| Mutex::new(None));
    *cell.lock().expect("lock is poisoned") = value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    // These tests assert std wall-clock behaviour, so they are gated behind
    // `feature = "std"`: under `--no-default-features` the `std` feature is off,
    // `SystemClock` is absent, and `unix_timestamp()` takes the no_std (epoch)
    // branch where these assertions would not hold.

    #[cfg(feature = "std")]
    #[test]
    fn system_clock_unix_time_is_after_a_known_recent_epoch() {
        // Any wall-clock reading taken after this fixed past instant (2023-11-14)
        // proves SystemClock consults the real system clock rather than a stub.
        let known_past = Duration::from_secs(1_700_000_000);
        assert!(
            SystemClock.unix_time() > known_past,
            "SystemClock must read the real system clock"
        );
    }

    // The no-override and override cases share one test so they cannot observe
    // each other's mutation of the process-global test override. (The suite runs
    // under nextest, which isolates each test in its own process, but keeping the
    // override roundtrip self-contained holds under any runner.)
    #[cfg(feature = "std")]
    #[test]
    fn unix_timestamp_honours_the_test_override() {
        // With no override registered, the free `unix_timestamp` helper reads
        // through the same SystemClock the rest of the engine uses.
        set_unix_timestamp_for_test(None);
        assert!(
            unix_timestamp() > Duration::from_secs(1_700_000_000),
            "without an override, unix_timestamp must read the real system clock"
        );

        // An override pins the value regardless of the wall-clock.
        let fixed = Duration::from_secs(42);
        set_unix_timestamp_for_test(Some(fixed));
        assert_eq!(unix_timestamp(), fixed);

        // Restore so later tests in this binary see the real clock again.
        set_unix_timestamp_for_test(None);
    }
}
