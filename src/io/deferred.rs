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
//! The first failed write is kept and returned by every later call; what was
//! queued behind it is dropped, as the file is the caller's to remove. A
//! panic of the inner writer is kept as a failure and the writer is called
//! no more.
//!
//! Without a spawner the writer is a plain buffering writer: its buffers go
//! down on the caller's thread.

use crate::table::writer::CompactionSpawner;
use alloc::collections::VecDeque;
use std::io::{self, Seek, SeekFrom, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// The bytes a lane holds before its writer drains it or waits.
const LANE_BYTES_MAX: usize = 1024 * 1024;

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
        Self {
            kind: error.kind(),
            message: error.to_string(),
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
}

struct Lane<W> {
    state: Mutex<State<W>>,
    /// Rung when a drain ends or takes a job off a full lane.
    progress: Condvar,
}

impl<W: Write> Lane<W> {
    /// A poisoned lock is taken as it is: the state is changed in single
    /// steps, none of which leaves it torn.
    fn lock(&self) -> MutexGuard<'_, State<W>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
            let state = self.lock();
            if !state.draining && (state.queue.is_empty() || state.inner.is_none()) {
                return state;
            }
            if state.draining {
                drop(
                    self.progress
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner),
                );
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
    lane: Arc<Lane<W>>,
    buf: Vec<u8>,
    capacity: usize,
    /// Whether a write that overflows the buffer first fills it, as
    /// [`super::FillingBufWriter`] does, or goes into the next one whole, as
    /// std's buffering writer does.
    fill: bool,
    spawner: Option<Arc<dyn CompactionSpawner>>,
}

impl<W: Write + Send + 'static> DeferredWriter<W> {
    /// Wraps `inner` with a `capacity`-byte buffer; `fill` as
    /// [`super::FillingBufWriter`] fills its buffer.
    pub(crate) fn with_capacity(capacity: usize, inner: W, fill: bool) -> Self {
        Self {
            lane: Arc::new(Lane {
                state: Mutex::new(State {
                    inner: Some(inner),
                    queue: VecDeque::new(),
                    queued_bytes: 0,
                    draining: false,
                    asked: false,
                    failed: None,
                    spare: Vec::new(),
                }),
                progress: Condvar::new(),
            }),
            buf: Vec::with_capacity(capacity),
            capacity,
            fill,
            spawner: None,
        }
    }

    /// [`Self::with_capacity`], under the name both file writers share.
    pub(crate) fn for_file(capacity: usize, inner: W, fill: bool) -> Self {
        Self::with_capacity(capacity, inner, fill)
    }

    /// Hands the buffers filled from here on to the workers of `spawner`.
    pub(crate) fn use_spawner(&mut self, spawner: Arc<dyn CompactionSpawner>) {
        self.spawner = Some(spawner);
    }

    /// Queues `job` behind what the lane holds and sees that it is drained:
    /// by a worker, or here when the lane is full and no thread drains it.
    fn queue(&self, job: Job<W>) -> io::Result<()> {
        let Some(spawner) = &self.spawner else {
            return self.run_here(job);
        };
        let bytes = match &job {
            Job::Write(buf) => buf.len(),
            Job::Run(_) => 0,
        };
        let mut state = self.lane.lock();
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
            let lane = Arc::clone(&self.lane);
            spawner.spawn(Box::new(move || {
                lane.lock().asked = false;
                lane.drain();
            }));
        }
        if full {
            self.make_room();
        }
        Ok(())
    }

    /// Brings a full lane back under its bound: drains it here when no
    /// thread does, waits for the one that does otherwise.
    fn make_room(&self) {
        loop {
            let state = self.lane.lock();
            if state.queued_bytes <= LANE_BYTES_MAX || state.inner.is_none() && !state.draining {
                return;
            }
            if state.draining {
                drop(
                    self.lane
                        .progress
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner),
                );
            } else {
                drop(state);
                self.lane.drain();
            }
        }
    }

    /// Runs `job` on the calling thread: the writer has no workers, so
    /// nothing is queued and the inner writer is in place.
    fn run_here(&self, job: Job<W>) -> io::Result<()> {
        let mut state = self.lane.lock();
        if let Some(failed) = &state.failed {
            return Err(failed.error());
        }
        let Some(inner) = state.inner.as_mut() else {
            return Err(io::Error::other("the file's writer panicked"));
        };
        let result = match job {
            Job::Write(mut buf) => {
                let result = inner.write_all(&buf);
                buf.clear();
                if state.spare.len() < SPARE_BUFFERS_MAX {
                    state.spare.push(buf);
                }
                result
            }
            Job::Run(run) => {
                run(inner);
                Ok(())
            }
        };
        if let Err(e) = &result {
            state.failed = Some(Failure::of(e));
        }
        result
    }

    /// Writes `data` on the calling thread, the writer having no workers.
    fn write_through(&self, data: &[u8]) -> io::Result<usize> {
        let mut state = self.lane.lock();
        if let Some(failed) = &state.failed {
            return Err(failed.error());
        }
        let Some(inner) = state.inner.as_mut() else {
            return Err(io::Error::other("the file's writer panicked"));
        };
        let result = inner.write_all(data);
        if let Err(e) = &result {
            state.failed = Some(Failure::of(e));
        }
        drop(state);
        result.map(|()| data.len())
    }

    /// Hands the buffer to the lane and takes an empty one.
    fn hand_over(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let spare = self.lane.lock().spare.pop();
        let next = spare.unwrap_or_else(|| Vec::with_capacity(self.capacity));
        let full = core::mem::replace(&mut self.buf, next);
        self.queue(Job::Write(full))
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
        self.queue(Job::Run(Box::new(run)))
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
        let mut state = self.lane.settle();
        if let Some(failed) = &state.failed {
            return Err(failed.error());
        }
        state
            .inner
            .take()
            .ok_or_else(|| io::Error::other("the file's writer panicked"))
    }

    /// Does `read` with the inner writer once everything written so far has
    /// reached it.
    ///
    /// # Errors
    ///
    /// Returns the first failure of a write.
    pub(crate) fn with_inner<R>(&mut self, read: impl FnOnce(&mut W) -> R) -> io::Result<R> {
        self.hand_over()?;
        let mut state = self.lane.settle();
        if let Some(failed) = &state.failed {
            return Err(failed.error());
        }
        state
            .inner
            .as_mut()
            .map(read)
            .ok_or_else(|| io::Error::other("the file's writer panicked"))
    }
}

impl<W: Write + Send + 'static> Write for DeferredWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let room = self.capacity - self.buf.len();
        if data.len() <= room {
            self.buf.extend_from_slice(data);
            return Ok(data.len());
        }
        // A write at least as large as the buffer goes down in a buffer of
        // its own, after what is buffered.
        if data.len() >= self.capacity {
            self.hand_over()?;
            if self.spawner.is_none() {
                // Written here anyway: as it lies, without a copy.
                return self.write_through(data);
            }
            self.queue(Job::Write(data.to_vec()))?;
            return Ok(data.len());
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
        // failure has no one to go to. The lane is left idle, so the inner
        // writer is dropped here and not later by a worker, and the caller
        // may remove the file.
        drop(self.hand_over());
        drop(self.lane.settle().inner.take());
    }
}

#[cfg(test)]
mod tests;
