// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The sequence numbers of synced flushes, and how far the manifest decides
//! them.
//!
//! A synced flush writes no manifest edit of its own: its tables describe
//! themselves (see [`crate::table::meta::FlushMark`]), its files are synced
//! before it returns, and the next install that writes the manifest names
//! them. Every manifest records the highest flush sequence number whose
//! outcome it decides: a flush up to it is named by the manifest if it is in
//! the tree, and gone if it is not (a compaction consumed it, a clear dropped
//! it, it failed). An open installs the self-described tables of the flushes
//! above it, which returned durable with no edit naming them yet.

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin provides the same Mutex API.
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// The tree's synced-flush sequence numbers.
#[derive(Debug)]
pub(crate) struct FlushSeqs(Mutex<State>);

#[derive(Debug)]
struct State {
    /// The number the next synced flush takes.
    next: u64,
    /// The number of the flush being written, which no manifest may decide
    /// yet: its tables may still be installed.
    in_flight: Option<u64>,
    /// The highest number a clear decided, the flush in flight at the time
    /// included: a clear drops what that flush writes, so no later manifest
    /// may leave it undecided again.
    cleared: u64,
}

impl FlushSeqs {
    /// The numbers after `decided`, the highest one a recovered manifest
    /// decides, which recovery refuses at `u64::MAX`.
    #[must_use]
    pub(crate) fn after(decided: u64) -> Self {
        debug_assert!(
            decided < u64::MAX,
            "recovery refuses a watermark at u64::MAX"
        );
        Self(Mutex::new(State {
            next: decided + 1,
            in_flight: None,
            cleared: decided,
        }))
    }

    /// Takes the next number for a synced flush about to write its tables.
    /// Flushes run one at a time, under the tree's flush lock.
    pub(crate) fn begin(&self) -> u64 {
        let mut state = self.0.lock();
        let seq = state.next;
        // One per flush from a number below `u64::MAX`: never reached.
        state.next = seq + 1;
        state.in_flight = Some(seq);
        seq
    }

    /// The flush that took a number is installed, failed or was declined.
    pub(crate) fn end(&self) {
        self.0.lock().in_flight = None;
    }

    /// A clear is about to persist: it decides every number taken, the flush
    /// in flight included, since what that flush writes was cleared.
    pub(crate) fn decide_taken(&self) {
        let mut state = self.0.lock();
        state.cleared = state.next - 1;
    }

    /// The highest number a manifest written now decides: every one below
    /// the flush being written, every one taken when none is, and never below
    /// what a clear decided.
    #[must_use]
    pub(crate) fn decided(&self) -> u64 {
        let state = self.0.lock();
        // Every number is at least 1: `after` starts above 0.
        (state.in_flight.unwrap_or(state.next) - 1).max(state.cleared)
    }
}

impl Default for FlushSeqs {
    fn default() -> Self {
        Self::after(0)
    }
}

#[cfg(test)]
mod tests;
