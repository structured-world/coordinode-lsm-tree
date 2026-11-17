// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A buffering writer that hands its full buffers to a worker.
//!
//! A table or blob file is written by the thread that builds it, and what the
//! kernel does for a write (the copy into the page cache, the pages it takes,
//! the blocks a writeback request places) is done on that thread too. A
//! [`DeferredWriter`] takes it off: a full buffer joins the file's lane, a
//! queue a worker drains in the order it was filled, and the writer goes on
//! into another buffer.
//!
//! ## Order
//!
//! One lane serves one file and at most one thread drains it at a time, so
//! the file takes its bytes in the order they were written, and a job queued
//! behind them (a writeback request for a range) finds them written.
//!
//! ## Backpressure and deadlock freedom
//!
//! The lane holds a bounded number of bytes. A writer that finds it full does
//! not wait on a worker that may never come: with no thread draining, it
//! drains the lane itself; with one draining, it waits for that thread, which
//! is running. A worker that arrives to find the lane drained or taken leaves.
//!
//! ## Failure
//!
//! The first failed write is kept and returned by every later call that
//! reaches the file: a hand-over of a buffer, a flush, a seek, a job, the
//! inner writer's return. A write that fits the buffer only fills it, as
//! with std's buffering writer, and is refused at the next of those, since
//! checking a lane's failure on every small write would lock the lane on the
//! hottest path. What was queued behind the failure is dropped, as the file
//! is the caller's to remove. A
//! panic of the inner writer is kept as a failure and the writer is called
//! no more.
//!
//! Without a spawner the writer is a plain buffering writer: its buffers go
//! down on the caller's thread.

use crate::table::writer::CompactionSpawner;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::io::{self, Seek, SeekFrom, Write};

/// The bytes a lane holds before its writer drains it or waits.
const LANE_BYTES_MAX: usize = 1024 * 1024;

/// The bytes a file writes on its writer's thread before its buffers go to
/// the workers. A file that ends below it is written as it always was: a
/// flush of 1 000 values has four buffers to hand over and nothing to gain,
/// and on one host its files' syncs, moved against each other by the
/// workers' writes, met a directory sync and took twice the flush's time
/// (9.0 to 20.2 µs per value), where flushes of 10 000 values and up took
/// 17 to 31% less.
const WORKERS_AFTER_BYTES: usize = 1024 * 1024;

/// `counted` plus `written`, counted no further than [`WORKERS_AFTER_BYTES`]:
/// a writer with no workers stays on the caller's thread and writes on, past
/// any count a `usize` holds on a 32-bit target. `counted` is at most the
/// threshold and `written` the length of a slice, so the sum fits.
fn count_up_to_threshold(counted: usize, written: usize) -> usize {
    (counted + written).min(WORKERS_AFTER_BYTES)
}

/// The buffers a lane keeps to hand back to its writer.
const SPARE_BUFFERS_MAX: usize = 4;

/// What a lane does for its file, in order.
enum Job<W> {
    /// Bytes to write.
    Write(Vec<u8>),
    /// Something to do with the file once the bytes before it are written.
    Run(Box<dyn FnOnce(&mut W) + Send>),
}

/// A failed write, kept to be returned again.
struct Failure {
    kind: io::ErrorKind,
    message: String,
}

impl Failure {
    fn of(error: &io::Error) -> Self {
        // A backend's error payload is formatted while the lane is held: a
        // panic there must not leave the drain marked, as a write's may not.
        let message = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| error.to_string()))
            .unwrap_or_else(|_| String::from("the file's writer failed with an unprintable error"));
        Self {
            kind: error.kind(),
            message,
        }
    }

    fn error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

struct State<W> {
    /// The inner writer, `None` while a thread drains the lane with it or
    /// once it panicked.
    inner: Option<W>,
    queue: VecDeque<Job<W>>,
    queued_bytes: usize,
    /// Whether a thread is draining the lane.
    draining: bool,
    /// Whether a worker was asked to drain the lane and has not arrived.
    asked: bool,
    failed: Option<Failure>,
    /// Emptied buffers, for the writer to fill again.
    spare: Vec<Vec<u8>>,
    /// The writer's buffer size: a buffer larger than it, which held a write
    /// that went down whole, is freed instead of kept.
    buffer: usize,
}

struct Lane<W> {
    state: Mutex<State<W>>,
    /// Rung when a drain ends or takes a job off a full lane.
    progress: Condvar,
}

impl<W: Write> Lane<W> {
    /// The state is changed in single steps under the lock, none of which
    /// leaves it torn should it unwind.
    fn lock(&self) -> MutexGuard<'_, State<W>> {
        self.state.lock()
    }

    /// Drains the lane on the calling thread unless another drains it,
    /// until it is empty.
    fn drain(&self) {
        let mut state = self.lock();
        if state.draining {
            return;
        }
        let Some(mut inner) = state.inner.take() else {
            return;
        };
        state.draining = true;
        let mut panicked = false;
        loop {
            let Some(job) = state.queue.pop_front() else {
                break;
            };
            let failed = state.failed.is_some();
            if let Job::Write(buf) = &job {
                state.queued_bytes -= buf.len();
            }
            drop(state);
            self.progress.notify_all();
            // What is queued behind a failure is dropped: the file is the
            // caller's to remove.
            let mut emptied = None;
            let result = if failed {
                Ok(())
            } else {
                std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| match job {
                    Job::Write(mut buf) => {
                        let result = inner.write_all(&buf);
                        buf.clear();
                        emptied = Some(buf);
                        result
                    }
                    Job::Run(run) => {
                        run(&mut inner);
                        Ok(())
                    }
                }))
                .unwrap_or_else(|_| {
                    panicked = true;
                    Err(io::Error::other("the file's writer panicked"))
                })
            };
            state = self.lock();
            if let Err(e) = result
                && state.failed.is_none()
            {
                state.failed = Some(Failure::of(&e));
            }
            if let Some(buf) = emptied
                && state.spare.len() < SPARE_BUFFERS_MAX
                && buf.capacity() <= state.buffer
            {
                state.spare.push(buf);
            }
            if panicked {
                state.queue.clear();
                state.queued_bytes = 0;
                break;
            }
        }
        // A writer that panicked is called no more, not even to be dropped
        // under the lock.
        if panicked {
            drop(state);
            drop(inner);
            state = self.lock();
        } else {
            state.inner = Some(inner);
        }
        state.draining = false;
        drop(state);
        self.progress.notify_all();
    }

    /// Waits until the lane is empty and no thread drains it, draining it
    /// on the calling thread when none does.
    fn settle(&self) -> MutexGuard<'_, State<W>> {
        loop {
            let mut state = self.lock();
            if !state.draining && (state.queue.is_empty() || state.inner.is_none()) {
                return state;
            }
            if state.draining {
                self.progress.wait(&mut state);
                drop(state);
            } else {
                drop(state);
                self.drain();
            }
        }
    }
}

/// A buffering writer whose full buffers a worker writes (see the module
/// docs). Without a spawner it writes them itself.
pub(crate) struct DeferredWriter<W: Write + Send + 'static> {
    sink: Sink<W>,
    buf: Vec<u8>,
    capacity: usize,
    /// Whether a write that overflows the buffer first fills it, as
    /// [`super::FillingBufWriter`] does, or goes into the next one whole, as
    /// std's buffering writer does.
    fill: bool,
    /// The first failed write of a writer that writes on the caller's
    /// thread; a lane keeps its own.
    failed: Option<Failure>,
    /// Whether a write on the caller's thread is under way: one that
    /// panicked leaves it set, and the drop then calls the writer no more.
    panicked: bool,
    /// The workers the buffers go to once the file has grown past
    /// [`WORKERS_AFTER_BYTES`] on the caller's thread, and what it has
    /// written there so far.
    workers: Option<Arc<dyn CompactionSpawner>>,
    written_here: usize,
}

/// Where a [`DeferredWriter`]'s buffers go.
enum Sink<W> {
    /// To the inner writer, on the caller's thread: one buffer, no lock, as
    /// a plain buffering writer.
    Here(W),
    /// To the inner writer's lane, drained by the workers of a spawner.
    Lane(Arc<Lane<W>>, Arc<dyn CompactionSpawner>),
    /// Nowhere: the inner writer was taken.
    Gone,
}

/// The error of a writer whose inner writer is gone.
fn gone() -> io::Error {
    io::Error::other("the file's writer panicked")
}

/// Queues `job` behind what `lane` holds and sees that it is drained: by a
/// worker of `spawner`, or here when the lane is full and no thread drains
/// it.
fn queue<W: Write + Send + 'static>(
    lane: &Arc<Lane<W>>,
    spawner: &Arc<dyn CompactionSpawner>,
    job: Job<W>,
) -> io::Result<()> {
    let bytes = match &job {
        Job::Write(buf) => buf.len(),
        Job::Run(_) => 0,
    };
    let mut state = lane.lock();
    if let Some(failed) = &state.failed {
        return Err(failed.error());
    }
    state.queue.push_back(job);
    state.queued_bytes += bytes;
    let ask = !state.draining && !state.asked;
    if ask {
        state.asked = true;
    }
    let full = state.queued_bytes > LANE_BYTES_MAX;
    drop(state);
    if ask {
        let lane = Arc::clone(lane);
        spawner.spawn(Box::new(move || {
            lane.lock().asked = false;
            lane.drain();
        }));
    }
    if full {
        make_room(lane);
    }
    Ok(())
}

/// Brings a full lane back under its bound: drains it here when no thread
/// does, waits for the one that does otherwise.
fn make_room<W: Write>(lane: &Lane<W>) {
    loop {
        let mut state = lane.lock();
        if state.queued_bytes <= LANE_BYTES_MAX || state.inner.is_none() && !state.draining {
            return;
        }
        if state.draining {
            lane.progress.wait(&mut state);
            drop(state);
        } else {
            drop(state);
            lane.drain();
        }
    }
}

impl<W: Write + Send + 'static> DeferredWriter<W> {
    /// Wraps `inner` with a `capacity`-byte buffer; `fill` as
    /// [`super::FillingBufWriter`] fills its buffer.
    pub(crate) fn with_capacity(capacity: usize, inner: W, fill: bool) -> Self {
        Self {
            sink: Sink::Here(inner),
            buf: Vec::with_capacity(capacity),
            capacity,
            fill,
            failed: None,
            panicked: false,
            workers: None,
            written_here: 0,
        }
    }

    /// [`Self::with_capacity`], under the name both file writers share.
    pub(crate) fn for_file(capacity: usize, inner: W, fill: bool) -> Self {
        Self::with_capacity(capacity, inner, fill)
    }

    /// Hands the file's buffers to the workers of `spawner` once it has
    /// written [`WORKERS_AFTER_BYTES`] on the caller's thread.
    pub(crate) fn use_spawner(&mut self, spawner: Arc<dyn CompactionSpawner>) {
        if let Sink::Lane(_, current) = &mut self.sink {
            *current = spawner;
        } else {
            self.workers = Some(spawner);
            self.take_on_workers();
        }
    }

    /// Hands the buffers filled from here on to the workers at once.
    #[cfg(test)]
    fn use_spawner_now(&mut self, spawner: Arc<dyn CompactionSpawner>) {
        self.written_here = WORKERS_AFTER_BYTES;
        self.use_spawner(spawner);
    }

    /// Moves the inner writer to a lane once the file has written enough on
    /// the caller's thread and has workers to go to.
    fn take_on_workers(&mut self) {
        if self.written_here < WORKERS_AFTER_BYTES || !matches!(self.sink, Sink::Here(_)) {
            return;
        }
        let Some(spawner) = self.workers.take() else {
            return;
        };
        // A write here that panicked, which the caller caught, fails the
        // file: the lane takes the failure with the writer.
        if self.panicked && self.failed.is_none() {
            self.failed = Some(Failure::of(&gone()));
        }
        self.sink = match core::mem::replace(&mut self.sink, Sink::Gone) {
            Sink::Here(inner) => Sink::Lane(
                Arc::new(Lane {
                    state: Mutex::new(State {
                        inner: Some(inner),
                        queue: VecDeque::new(),
                        queued_bytes: 0,
                        draining: false,
                        asked: false,
                        failed: self.failed.take(),
                        spare: Vec::new(),
                        buffer: self.capacity,
                    }),
                    progress: Condvar::new(),
                }),
                spawner,
            ),
            Sink::Lane(lane, _) => Sink::Lane(lane, spawner),
            Sink::Gone => Sink::Gone,
        };
    }

    /// Writes `data` to `inner` on the calling thread.
    fn write_here(
        inner: &mut W,
        (failed, panicked): (&mut Option<Failure>, &mut bool),
        data: &[u8],
    ) -> io::Result<()> {
        if let Some(failed) = failed {
            return Err(failed.error());
        }
        *panicked = true;
        let result = inner.write_all(data);
        *panicked = false;
        if let Err(e) = &result {
            *failed = Some(Failure::of(e));
        }
        result
    }

    /// Hands the buffer on: to the inner writer here, or to its lane, taking
    /// an empty buffer in its place.
    fn hand_over(&mut self) -> io::Result<()> {
        // A write here that panicked, which the caller caught, is a failure
        // of the file: the interrupted buffer is not written again, and a
        // call with nothing buffered does not answer as if the file were
        // whole.
        if self.panicked && self.failed.is_none() {
            self.failed = Some(Failure::of(&gone()));
        }
        if self.buf.is_empty() {
            return Ok(());
        }
        match &mut self.sink {
            Sink::Here(inner) => {
                let result =
                    Self::write_here(inner, (&mut self.failed, &mut self.panicked), &self.buf);
                self.written_here = count_up_to_threshold(self.written_here, self.buf.len());
                self.buf.clear();
                self.take_on_workers();
                result
            }
            Sink::Lane(lane, spawner) => {
                let spare = lane.lock().spare.pop();
                let next = spare.unwrap_or_else(|| Vec::with_capacity(self.capacity));
                let full = core::mem::replace(&mut self.buf, next);
                queue(lane, spawner, Job::Write(full))
            }
            Sink::Gone => Err(gone()),
        }
    }

    /// Does `run` with the inner writer once every byte written so far has
    /// reached it, without waiting for that: on a worker when the writer has
    /// them.
    ///
    /// # Errors
    ///
    /// Returns the failure of an earlier write.
    pub(crate) fn after_writes(
        &mut self,
        run: impl FnOnce(&mut W) + Send + 'static,
    ) -> io::Result<()> {
        self.hand_over()?;
        match &mut self.sink {
            Sink::Here(inner) => {
                if let Some(failed) = &self.failed {
                    return Err(failed.error());
                }
                run(inner);
                Ok(())
            }
            Sink::Lane(lane, spawner) => queue(lane, spawner, Job::Run(Box::new(run))),
            Sink::Gone => Err(gone()),
        }
    }

    /// Writes out what is buffered and queued and hands back the inner
    /// writer.
    ///
    /// # Errors
    ///
    /// Returns the first failure of a write.
    #[cfg(test)]
    pub(crate) fn finish(mut self) -> io::Result<W> {
        self.hand_over()?;
        match core::mem::replace(&mut self.sink, Sink::Gone) {
            Sink::Here(inner) => match &self.failed {
                Some(failed) => Err(failed.error()),
                None => Ok(inner),
            },
            Sink::Lane(lane, _) => {
                let mut state = lane.settle();
                if let Some(failed) = &state.failed {
                    return Err(failed.error());
                }
                state.inner.take().ok_or_else(gone)
            }
            Sink::Gone => Err(gone()),
        }
    }

    /// Does `read` with the inner writer once everything written so far has
    /// reached it.
    ///
    /// # Errors
    ///
    /// Returns the first failure of a write.
    pub(crate) fn with_inner<R>(&mut self, read: impl FnOnce(&mut W) -> R) -> io::Result<R> {
        self.hand_over()?;
        match &mut self.sink {
            Sink::Here(inner) => match &self.failed {
                Some(failed) => Err(failed.error()),
                None => Ok(read(inner)),
            },
            Sink::Lane(lane, _) => {
                let mut state = lane.settle();
                if let Some(failed) = &state.failed {
                    return Err(failed.error());
                }
                state.inner.as_mut().map(read).ok_or_else(gone)
            }
            Sink::Gone => Err(gone()),
        }
    }

    /// The bytes the lane holds, `0` for a writer without one.
    #[cfg(test)]
    fn queued_bytes(&self) -> usize {
        match &self.sink {
            Sink::Lane(lane, _) => lane.lock().queued_bytes,
            Sink::Here(_) | Sink::Gone => 0,
        }
    }
}

impl<W: Write + Send + 'static> Write for DeferredWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // A buffer left exactly full by the last write has no room to take
        // a byte of this one: it goes down first, or a writer that fills
        // would take nothing of the write.
        if self.buf.len() == self.capacity && !data.is_empty() {
            self.hand_over()?;
        }
        let room = self.capacity - self.buf.len();
        if data.len() <= room {
            self.buf.extend_from_slice(data);
            return Ok(data.len());
        }
        // A write at least as large as the buffer goes down after what is
        // buffered: as it lies on the caller's thread, in a buffer of its
        // own on a lane.
        if data.len() >= self.capacity {
            self.hand_over()?;
            return match &mut self.sink {
                Sink::Here(inner) => {
                    let result =
                        Self::write_here(inner, (&mut self.failed, &mut self.panicked), data);
                    self.written_here = count_up_to_threshold(self.written_here, data.len());
                    self.take_on_workers();
                    result
                }
                Sink::Lane(lane, spawner) => queue(lane, spawner, Job::Write(data.to_vec())),
                Sink::Gone => Err(gone()),
            }
            .map(|()| data.len());
        }
        if self.fill {
            self.buf
                .extend_from_slice(data.get(..room).unwrap_or_default());
            self.hand_over()?;
            return Ok(room);
        }
        self.hand_over()?;
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    /// Writes out what is buffered and queued, and flushes the inner writer.
    fn flush(&mut self) -> io::Result<()> {
        self.with_inner(Write::flush)?
    }
}

impl<W: Write + Seek + Send + 'static> Seek for DeferredWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // What is buffered and queued belongs before the seek target.
        self.with_inner(|inner| inner.seek(pos))?
    }
}

impl<W: Write + Send + 'static> Drop for DeferredWriter<W> {
    fn drop(&mut self) {
        // As std's buffering writer, what is buffered is written, and a
        // failure has no one to go to; a writer whose write panicked is not
        // called again. A lane is left idle, so the inner writer is dropped
        // here and not later by a worker, and the caller may remove the file.
        if !self.panicked {
            drop(self.hand_over());
        }
        if let Sink::Lane(lane, _) = &self.sink {
            drop(lane.settle().inner.take());
        }
    }
}

#[cfg(test)]
mod tests;
