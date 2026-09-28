// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Ordered execution of a writer's jobs on a caller-injected executor.
//!
//! One thread, the writer, submits jobs in the order their results must be
//! consumed; any number of workers run them in any order; the writer takes the
//! results back strictly in submission order. What is ordered is the
//! consumption, not the work.
//!
//! ## What a job costs to coordinate
//!
//! A worker is reached through a token task spawned on the executor, and a
//! token does not run one job: it keeps taking jobs from the shared queue until
//! the queue is empty, then exits. At most `concurrency` tokens are live, so a
//! stream of jobs costs one spawn per worker that joins in, not one per job.
//! A finished result goes into a ring of `capacity` slots indexed by its
//! sequence number, so publishing and taking it neither allocates nor searches.
//! A worker publishes its result and claims its next job under one lock.
//!
//! ## Deadlock freedom (help-first draining)
//!
//! When the writer needs a result that is not ready, it first claims and runs
//! queued jobs on its own thread, and parks only once the queue is empty, when
//! every job still outstanding is running on a worker. A writer running on one
//! of the executor's own threads, or against a saturated or one-worker pool,
//! therefore degrades to running every job itself instead of waiting on a token
//! that cannot start.
//!
//! ## Capacity
//!
//! The writer never has more than `capacity` results outstanding (submitted
//! and not yet taken): it takes one before it submits past that. That is what
//! makes a ring of `capacity` slots enough.

use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
};

use super::parallel_compressor::CompactionSpawner;

/// Work an [`OrderedPipeline`] runs: consumed by the thread that runs it,
/// reading the context every job of the pipeline shares.
pub trait OrderedJob: Send + 'static {
    /// What every job of one pipeline reads, fixed when the pipeline is built.
    type Context: Send + Sync + 'static;
    /// What a job produces, taken back by the writer in submission order.
    type Output: Send + 'static;

    /// Runs the job.
    fn run(self, context: &Self::Context) -> Self::Output;
}

/// What the writer and the workers share under one lock.
struct State<J: OrderedJob> {
    /// Jobs submitted and not yet claimed, oldest first, with their sequence
    /// numbers.
    queue: VecDeque<(u64, J)>,
    /// Finished results not yet taken, each at its sequence number modulo the
    /// ring's length.
    slots: Box<[Option<J::Output>]>,
    /// Tokens spawned and not yet exited.
    tokens: usize,
    /// Tokens parked waiting for a job to be queued.
    idle: usize,
    /// Set when the pipeline is dropped: parked tokens exit.
    closed: bool,
    /// Whether the writer is parked waiting for a result, so a worker that
    /// publishes one has someone to wake.
    writer_waiting: bool,
}

impl<J: OrderedJob> State<J> {
    /// The ring slot of sequence number `seq`.
    fn slot(&mut self, seq: u64) -> Option<&mut Option<J::Output>> {
        // A remainder of the slot count fits a `usize`.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a remainder of a usize-sized ring length"
        )]
        let at = (seq % self.slots.len() as u64) as usize;
        self.slots.get_mut(at)
    }

    /// Stores `output` as the result of `seq`, returning whether the writer is
    /// waiting to be woken.
    fn publish(&mut self, seq: u64, output: J::Output) -> bool {
        if let Some(slot) = self.slot(seq) {
            // The writer takes a result before it submits a job whose slot
            // would be the same, so the slot is free.
            debug_assert!(slot.is_none(), "an ordered pipeline slot was reused");
            *slot = Some(output);
        }
        self.writer_waiting
    }
}

/// The pipeline's shared half, held by the writer and every live token.
struct Shared<J: OrderedJob> {
    state: Mutex<State<J>>,
    /// The writer parks here for a result.
    woke: Condvar,
    /// Idle tokens park here for a job.
    queued: Condvar,
    /// The writer's thread: a token run there does not park, since only that
    /// thread could queue the job it would wait for.
    writer: std::thread::ThreadId,
    context: J::Context,
}

/// How long a token that finds the queue empty stays parked for the next job
/// before it gives its thread back to the executor.
const LINGER: std::time::Duration = std::time::Duration::from_millis(1);

impl<J: OrderedJob> Shared<J> {
    fn lock(&self) -> MutexGuard<'_, State<J>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A token's body: runs queued jobs, publishing each result and claiming
    /// the next job under one lock. When the queue is empty it parks for up to
    /// [`LINGER`] for the next one, then exits.
    fn work(&self) {
        let may_park = std::thread::current().id() != self.writer;
        let mut done: Option<(u64, J::Output)> = None;
        loop {
            let mut state = self.lock();
            if done
                .take()
                .is_some_and(|(seq, output)| state.publish(seq, output))
            {
                // Before parking: the writer must not wait out the linger.
                self.woke.notify_one();
            }
            let mut next = state.queue.pop_front();
            if next.is_none() && may_park && !state.closed {
                state.idle += 1;
                state = self
                    .queued
                    .wait_timeout_while(state, LINGER, |s| s.queue.is_empty() && !s.closed)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
                state.idle -= 1;
                next = state.queue.pop_front();
            }
            if next.is_none() {
                // Leaves under the lock the submitter checks, so a job queued
                // after this sees one token fewer and spawns one.
                state.tokens -= 1;
            }
            drop(state);
            let Some((seq, job)) = next else {
                return;
            };
            done = Some((seq, job.run(&self.context)));
        }
    }

    /// Claims and runs one queued job on the calling thread, the writer's,
    /// publishing its result. `false` when the queue is empty.
    fn help(&self) -> bool {
        let Some((seq, job)) = self.lock().queue.pop_front() else {
            return false;
        };
        let output = job.run(&self.context);
        // The writer is the thread helping, so nobody waits to be woken.
        self.lock().publish(seq, output);
        true
    }
}

/// Ordered execution of one writer's jobs on an injected executor.
pub struct OrderedPipeline<J: OrderedJob> {
    spawner: Arc<dyn CompactionSpawner>,
    shared: Arc<Shared<J>>,
    /// The most tokens live at once.
    concurrency: usize,
    /// The ring's length: the most results outstanding at once.
    capacity: usize,
    next_submit: u64,
    next_drain: u64,
}

impl<J: OrderedJob> OrderedPipeline<J> {
    /// A pipeline running on `spawner` with up to `concurrency` workers, for a
    /// writer that keeps at most `capacity` results outstanding. Both are at
    /// least one.
    pub fn new(
        spawner: Arc<dyn CompactionSpawner>,
        context: J::Context,
        concurrency: usize,
        capacity: usize,
    ) -> Self {
        let capacity = capacity.max(1);
        let slots = core::iter::repeat_with(|| None).take(capacity).collect();
        Self {
            spawner,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    queue: VecDeque::with_capacity(capacity),
                    slots,
                    tokens: 0,
                    idle: 0,
                    closed: false,
                    writer_waiting: false,
                }),
                woke: Condvar::new(),
                queued: Condvar::new(),
                writer: std::thread::current().id(),
                context,
            }),
            concurrency: concurrency.max(1),
            capacity,
            next_submit: 0,
            next_drain: 0,
        }
    }

    /// Results submitted and not yet taken.
    pub fn pending(&self) -> usize {
        // Taking never outruns submitting, and the difference is bounded by
        // the ring's `usize` length.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "bounded by the ring's usize length"
        )]
        let pending = (self.next_submit - self.next_drain) as usize;
        pending
    }

    /// Submits `job` to run on a worker.
    pub fn submit(&mut self, job: J) {
        let seq = self.take_seq();
        let spawn = {
            let mut state = self.shared.lock();
            state.queue.push_back((seq, job));
            if state.idle > 0 {
                self.shared.queued.notify_one();
            }
            // A parked token takes one queued job each; spawn only for jobs
            // beyond what they cover.
            let spawn = state.tokens < self.concurrency && state.queue.len() > state.idle;
            if spawn {
                state.tokens += 1;
            }
            spawn
        };
        if spawn {
            let shared = Arc::clone(&self.shared);
            self.spawner.spawn(Box::new(move || shared.work()));
        }
    }

    /// Runs `job` on the calling thread and keeps its result in order with the
    /// rest: for work too small to be worth a worker.
    pub fn submit_inline(&mut self, job: J) {
        let seq = self.take_seq();
        let output = job.run(&self.shared.context);
        self.shared.lock().publish(seq, output);
    }

    fn take_seq(&mut self) -> u64 {
        debug_assert!(
            self.pending() < self.capacity,
            "more results outstanding than the ring holds",
        );
        let seq = self.next_submit;
        self.next_submit += 1;
        seq
    }

    /// The next result in submission order, running queued jobs on this thread
    /// while it is not ready. `None` only when nothing is outstanding.
    pub fn take_next(&mut self) -> Option<J::Output> {
        if self.next_drain == self.next_submit {
            return None;
        }
        let seq = self.next_drain;
        loop {
            let ready = self.shared.lock().slot(seq).and_then(Option::take);
            if let Some(output) = ready {
                self.next_drain += 1;
                return Some(output);
            }
            // Jobs are claimed oldest first and `seq` is the oldest not taken,
            // so the first job helped is `seq` itself unless a worker has it.
            if self.shared.help() {
                continue;
            }
            // The queue is empty: `seq` is running on a worker (the writer is
            // the only submitter, so nothing is queued while it waits here).
            let mut state = self.shared.lock();
            loop {
                if let Some(output) = state.slot(seq).and_then(Option::take) {
                    state.writer_waiting = false;
                    self.next_drain += 1;
                    return Some(output);
                }
                state.writer_waiting = true;
                state = self
                    .shared
                    .woke
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
}

impl<J: OrderedJob> Drop for OrderedPipeline<J> {
    fn drop(&mut self) {
        self.shared.lock().closed = true;
        self.shared.queued.notify_all();
    }
}

#[cfg(test)]
mod tests;
