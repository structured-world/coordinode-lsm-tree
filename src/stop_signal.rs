// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use alloc::sync::Arc;
#[cfg(feature = "std")]
use alloc::vec::Vec;
use core::sync::atomic::AtomicBool;

#[derive(Clone, Debug, Default)]
pub struct StopSignal {
    stopped: Arc<AtomicBool>,
    /// The rate limiters whose waiters a stop wakes: a throttled wait sleeps
    /// until its deadline, and only a ring makes it look at this signal
    /// sooner.
    // no-std: the no_std limiter requests never wait
    #[cfg(feature = "std")]
    wakeups: Arc<parking_lot::Mutex<Vec<Arc<crate::rate_limiter::Wakeup>>>>,
}

impl StopSignal {
    pub fn send(&self) {
        self.stopped
            .store(true, core::sync::atomic::Ordering::Release);
        // After the store, so a woken waiter sees the stop.
        #[cfg(feature = "std")]
        for wakeup in self.wakeups.lock().iter() {
            wakeup.ring();
        }
    }

    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(core::sync::atomic::Ordering::Acquire)
    }

    /// Makes a stop sent through this signal wake the requests waiting on
    /// `limiter`, so a throttled compaction of this tree stops at once.
    #[cfg(feature = "std")]
    pub(crate) fn wake_on_stop(&self, limiter: &crate::rate_limiter::RateLimiter) {
        self.wakeups.lock().push(limiter.wakeup());
    }
}
