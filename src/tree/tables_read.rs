// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! The read of a key batch through a version's levels as a machine: each
//! level is resolved from its staged plan, top down, and a level that cannot
//! be is handed out whole as a job that resolves it key by key. Like the
//! level it drives, it never opens, reads or waits itself.

use super::level_resolve::{JobDone, LevelJob, LevelResolve, LevelWork, ReadMachine};
use crate::version::Version;
use crate::{InternalValue, SeqNo};
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

/// The read of the keys of `remaining` through the levels of a version, at
/// the snapshot `seqno`, keeping per key the newest version the first level
/// holding it has.
pub(super) struct TablesRead<'a, 'k, K> {
    version: &'a Version,
    keys: &'k [K],
    seqno: SeqNo,
    comparator: &'a dyn crate::comparator::UserComparator,
    metadata_budget: u64,
    /// The next level to read.
    next: usize,
    /// `(key index, filter hash)` of the keys no level above answered, sorted
    /// under `comparator`; held by the level being read while it is.
    remaining: Vec<(usize, u64)>,
    state: State,
    /// The level resolved from its staged plan, while one is.
    staged: Option<LevelResolve<'a, 'k, K>>,
}

impl<'a, 'k, K: AsRef<[u8]>> TablesRead<'a, 'k, K> {
    /// The read of the keys of `remaining` (indices into `keys` with their
    /// filter hashes, sorted under `comparator`) through `version`.
    pub(super) fn new(
        version: &'a Version,
        keys: &'k [K],
        remaining: Vec<(usize, u64)>,
        seqno: SeqNo,
        comparator: &'a dyn crate::comparator::UserComparator,
        metadata_budget: u64,
    ) -> Self {
        Self {
            version,
            keys,
            seqno,
            comparator,
            metadata_budget,
            next: 0,
            remaining,
            state: State::Between,
            staged: None,
        }
    }
}

impl<'a, 'k, K: AsRef<[u8]>> ReadMachine<'a, 'k> for TablesRead<'a, 'k, K> {
    /// The first failure of a level resolved key by key: the staged resolve
    /// hands its own failures to that resolve rather than reporting them.
    type Output = crate::Result<()>;

    fn pump(
        &mut self,
        results: &mut [Option<InternalValue>],
        out: &mut LevelWork<'a, 'k>,
    ) -> Option<crate::Result<()>> {
        loop {
            match &mut self.state {
                State::Between => {
                    let level = match self.version.level(self.next) {
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
                    self.next += 1;
                    self.staged = Some(LevelResolve::new(
                        level,
                        core::mem::take(&mut self.remaining),
                        self.keys,
                        self.comparator,
                        self.seqno,
                        self.metadata_budget,
                    ));
                    self.state = State::Staged;
                }
                State::Staged => {
                    let Some(level) = &mut self.staged else {
                        unreachable!("a staged level is held while it is read");
                    };
                    let resolved = ReadMachine::pump(level, results, out)?;
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
                        let level_zero = self.next == 1;
                        let Some(level) = self.version.level(self.next - 1) else {
                            unreachable!("the level was just read");
                        };
                        out.jobs.push(LevelJob::Serial {
                            level_zero,
                            level,
                            remaining: core::mem::take(&mut self.remaining),
                            keys: self.keys.iter().map(AsRef::as_ref).collect(),
                            seqno: self.seqno,
                            comparator: self.comparator,
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

    fn job_done(&mut self, done: JobDone) {
        match (&mut self.state, &mut self.staged, done) {
            (State::Serial(slot @ None), _, JobDone::Serial { result }) => *slot = Some(result),
            (State::Staged, Some(level), done) => level.job_done(done),
            _ => debug_assert!(false, "a job handed back to a read that did not ask for it"),
        }
    }

    fn read_done(&mut self, done: crate::fs::ReadDone) {
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
