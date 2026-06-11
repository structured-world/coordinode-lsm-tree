// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

/// Gets the unix timestamp as a duration (wall-clock time since the epoch).
///
/// Under `std` this reads the system clock. Under `no_std` there is no ambient
/// clock, so the value comes from a caller-registered hook (see
/// [`set_clock`]); until one is registered it is [`core::time::Duration::ZERO`]
/// (epoch), which disables TTL expiry rather than expiring everything.
pub fn unix_timestamp() -> core::time::Duration {
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
        let now = std::time::SystemTime::now();
        #[expect(clippy::expect_used, reason = "trivial")]
        now.duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("time went backwards")
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

/// Registers the wall-clock source used by [`unix_timestamp`] under `no_std`
/// (e.g. a WASM host's `Date.now()`). Idempotent — the first registration
/// wins; later calls are ignored. A no-op under `std`, where the system clock
/// is always available.
#[cfg(not(feature = "std"))]
pub fn set_clock(clock: fn() -> core::time::Duration) {
    nostd_clock::set(clock);
}

#[cfg(not(feature = "std"))]
mod nostd_clock {
    use alloc::boxed::Box;
    use core::time::Duration;
    use once_cell::race::OnceBox;

    // Caller-injected wall-clock. Lock-free (atomic pointer), set once.
    static CLOCK: OnceBox<fn() -> Duration> = OnceBox::new();

    pub fn set(clock: fn() -> Duration) {
        let _ = CLOCK.set(Box::new(clock));
    }

    pub fn now() -> Duration {
        CLOCK.get().map_or(Duration::ZERO, |clock| clock())
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
