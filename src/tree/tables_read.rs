// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! The read of a key batch through a version's levels as a machine: each
//! level is resolved from its staged plan, top down: every table's filter
//! blocks in one batch, then its index blocks, then its data blocks, and
//! answered from the blocks read, so on `io_uring` each batch is one
//! submission the kernel fans out across the underlying devices. A level
//! that cannot be resolved so is handed out whole as a job that resolves it
//! key by key. Like the level it drives, it never opens, reads or waits
//! itself.

use super::level_resolve::{LevelResolve, LevelStep};
use super::read_job::{Job, JobDone, ReadCtx, ReadSink};
use crate::InternalValue;
use alloc::vec::Vec;

/// What resolving a level key by key gives: the versions found, by key
/// index, and the keys left for the levels below, still sorted.
pub(super) type SerialLevel = (Vec<(usize, InternalValue)>, Vec<(usize, u64)>);

enum State {
    /// No level is being read: the next one starts on the next pump.
    Between,
    /// A level is resolved from its staged plan, held in `staged`.
    Staged,
    /// A level is resolved key by key, by a job handed out; `None` until the
    /// job is back.
    Serial(Option<crate::Result<SerialLevel>>),
    Over,
}

/// The read of the keys of `remaining` through the levels of the read's
/// version, keeping per key the newest version the first level holding it
/// has.
pub(super) struct TablesRead<'a, K> {
    ctx: &'a ReadCtx<K>,
    /// The next level to read.
    next: usize,
    /// `(key index, filter hash)` of the keys no level above answered, sorted
    /// under the read's comparator; held by the level being read while it is.
    remaining: Vec<(usize, u64)>,
    state: State,
    /// The level resolved from its staged plan, while one is.
    staged: Option<LevelResolve<'a, K>>,
}

impl<'a, K: AsRef<[u8]>> TablesRead<'a, K> {
    /// The read of the keys of `remaining` (indices into the read's keys with
    /// their filter hashes, sorted under its comparator).
    pub(super) const fn new(ctx: &'a ReadCtx<K>, remaining: Vec<(usize, u64)>) -> Self {
        Self {
            ctx,
            next: 0,
            remaining,
            state: State::Between,
            staged: None,
        }
    }

    /// Moves the read on as far as what is back takes it, keeping in
    /// `results` the newest version per key the first level holding it has,
    /// and asking `out` for the work it needs next; `Some` once every level
    /// is read and nothing is out, with the first failure of a level resolved
    /// key by key (the staged resolve hands its own failures to that resolve
    /// rather than reporting them).
    pub(super) fn pump(
        &mut self,
        results: &mut [Option<InternalValue>],
        out: &mut impl ReadSink<'a>,
    ) -> Option<crate::Result<()>> {
        loop {
            match &mut self.state {
                State::Between => {
                    let level = match self.ctx.version().level(self.next) {
                        Some(level) if !self.remaining.is_empty() => level,
                        _ => {
                            self.state = State::Over;
                            return Some(Ok(()));
                        }
                    };
                    // The keys still to resolve have no answer yet, so a level
                    // handed back is restored by clearing theirs.
                    debug_assert!(
                        self.remaining
                            .iter()
                            .all(|&(idx, _)| results.get(idx).is_some_and(Option::is_none))
                    );
                    self.staged = Some(LevelResolve::new(
                        level,
                        self.next,
                        core::mem::take(&mut self.remaining),
                        &self.ctx.keys,
                        self.ctx.comparator.as_ref(),
                        self.ctx.seqno,
                        self.ctx.metadata_budget,
                    ));
                    self.next += 1;
                    self.state = State::Staged;
                }
                State::Staged => {
                    let Some(level) = &mut self.staged else {
                        unreachable!("a staged level is held while it is read");
                    };
                    let resolved = match level.pump(results, out) {
                        LevelStep::Pending => return None,
                        LevelStep::Resolved => true,
                        LevelStep::Serial => false,
                    };
                    let Some(level) = self.staged.take() else {
                        unreachable!("matched above");
                    };
                    self.state = State::Between;
                    self.remaining = level.into_remaining();
                    if !resolved {
                        // The level is resolved key by key, from no answer:
                        // the staged resolve cleared what it had set. A table
                        // the staged resolve could not plan may be one this
                        // never reads (a key an earlier level-0 run holds at
                        // the read's ceiling skips the later runs), so it fails
                        // only where a key-by-key read would; either way the
                        // level is answered, never skipped for a lower one.
                        out.job(Job::Serial {
                            level: self.next - 1,
                            remaining: core::mem::take(&mut self.remaining),
                        });
                        self.state = State::Serial(None);
                        return None;
                    }
                }
                State::Serial(None) => return None,
                State::Serial(Some(_)) => {
                    let State::Serial(Some(result)) =
                        core::mem::replace(&mut self.state, State::Between)
                    else {
                        unreachable!("matched above");
                    };
                    let (found, remaining) = match result {
                        Ok(level) => level,
                        Err(error) => {
                            self.state = State::Over;
                            return Some(Err(error));
                        }
                    };
                    for (idx, item) in found {
                        if let Some(slot) = results.get_mut(idx) {
                            *slot = Some(item);
                        }
                    }
                    self.remaining = remaining;
                }
                State::Over => return Some(Ok(())),
            }
        }
    }

    /// Takes back a finished job, asking `out` for the work it unblocks.
    pub(super) fn job_done(&mut self, done: JobDone, out: &mut impl ReadSink<'a>) {
        match (&mut self.state, &mut self.staged, done) {
            (State::Serial(slot @ None), _, JobDone::Serial { result }) => *slot = Some(result),
            (State::Staged, Some(level), done) => level.job_done(done, out),
            _ => debug_assert!(false, "a job handed back to a read that did not ask for it"),
        }
    }

    /// Takes back a finished block read.
    pub(super) fn read_done(&mut self, done: crate::fs::ReadDone) {
        if let Some(level) = &mut self.staged {
            level.read_done(done);
        } else {
            debug_assert!(
                false,
                "a read handed back to a read that did not ask for it"
            );
        }
    }
}
