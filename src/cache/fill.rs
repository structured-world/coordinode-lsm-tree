// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Cache fills a read leaves to a worker of the cache's own.
//!
//! A read of a packed value alone that decides to cache it hands it here and
//! returns: the entry is built and inserted, and whatever the insert evicts
//! is evicted, on the worker, so a read pays a push onto a bounded queue
//! rather than the insert and the eviction pass it may set off. The cache is
//! a hint, so a fill that finds the queue full is dropped, and one still
//! queued when the next read of the same value misses lets that read read
//! its record again, one small read. A frame is not left here: the values
//! beside the one read are read next, and each would decode it again.
//!
//! While fills keep coming the worker drains the queue every [`POLL`] and a
//! read never wakes it; once it has found the queue empty for [`IDLE_POLLS`]
//! drains in a row it parks, and the next fill wakes it.

use super::{CacheKey, Data, FillStage, Item};
use crate::Slice;
use crate::sharded_cache::Priority;
use crate::vlog::blob_file::packed::Record;
use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use spin::Mutex;

/// The most fills waiting; past it a fill is dropped.
const QUEUE: usize = 4_096;
/// How long the worker waits between drains while fills keep coming.
const POLL: Duration = Duration::from_micros(100);
/// Empty drains in a row before the worker parks until a fill wakes it.
const IDLE_POLLS: u32 = 64;

/// A packed value read alone, as a read leaves it to be cached: the record,
/// its bytes as read and its value decoded, at offset `index` of its packed
/// block, its entry built by the worker.
pub(crate) struct Fill {
    pub(crate) key: CacheKey,
    pub(crate) record: Record,
    pub(crate) bytes: Slice,
    pub(crate) value: Slice,
    pub(crate) index: u16,
    /// Experiment: how far the worker takes the fill.
    pub(crate) stage: FillStage,
}

impl Fill {
    /// Builds the entry and inserts it, on probation (see
    /// [`super::Cache::fill_blob_record`]).
    pub(crate) fn apply(self, data: &Data) {
        if self.stage == FillStage::DropOnly {
            return;
        }
        let frame = crate::vlog::blob_file::packed::record_frame(
            &self.record,
            &self.bytes,
            self.value,
            self.index,
        );
        let item = Item::BlobFrame(Arc::new(frame));
        match self.stage {
            FillStage::RoomOnly => data.insert_if_room(self.key, item, Priority::Probation),
            FillStage::Insert => data.insert_with_priority(self.key, item, Priority::Probation),
            FillStage::BuildOnly
            | FillStage::DropOnly
            | FillStage::Inline
            | FillStage::InlineTry => drop(core::hint::black_box(item)),
        }
    }
}

/// What a read and the worker share.
struct Shared {
    queue: Mutex<Vec<Fill>>,
    /// Whether the worker is parked, so the next fill wakes it.
    parked: AtomicBool,
    /// Set as the cache drops: the worker exits at its next look.
    closed: AtomicBool,
    /// Whether the worker holds fills taken off the queue and not all made.
    busy: AtomicBool,
}

/// The fill queue of one cache and its worker, started by the first fill.
pub(crate) struct Fills {
    shared: Arc<Shared>,
    worker: once_cell::race::OnceBox<std::thread::Thread>,
    /// Held while the worker starts, so two first fills start one.
    starting: Mutex<()>,
    /// Whether the worker could not be started: fills are then made inline.
    inline: AtomicBool,
}

impl Fills {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(Vec::new()),
                parked: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                busy: AtomicBool::new(false),
            }),
            worker: once_cell::race::OnceBox::new(),
            starting: Mutex::new(()),
            inline: AtomicBool::new(false),
        }
    }

    /// Leaves `fill` to the worker, or makes it now when no worker runs.
    pub(crate) fn push(&self, data: &Arc<Data>, fill: Fill) {
        if self.inline.load(Ordering::Relaxed) {
            fill.apply(data);
            return;
        }
        let Some(worker) = self.worker(data) else {
            fill.apply(data);
            return;
        };
        {
            let mut queue = self.shared.queue.lock();
            if queue.len() >= QUEUE {
                return;
            }
            queue.push(fill);
        }
        if self.shared.parked.load(Ordering::Acquire) {
            worker.unpark();
        }
    }

    /// Waits until every fill pushed before the call is made or dropped.
    pub(crate) fn settle(&self) {
        while self.worker.get().is_some()
            && (self.shared.busy.load(Ordering::Acquire) || !self.shared.queue.lock().is_empty())
        {
            if let Some(worker) = self.worker.get() {
                worker.unpark();
            }
            std::thread::sleep(POLL);
        }
    }

    /// The worker's handle, started on the first call; `None` when it could
    /// not be.
    fn worker(&self, data: &Arc<Data>) -> Option<&std::thread::Thread> {
        if let Some(worker) = self.worker.get() {
            return Some(worker);
        }
        let _starting = self.starting.lock();
        if let Some(worker) = self.worker.get() {
            return Some(worker);
        }
        if self.inline.load(Ordering::Relaxed) {
            return None;
        }
        let shared = Arc::clone(&self.shared);
        let data = Arc::clone(data);
        match std::thread::Builder::new()
            .name("lsm-cache-fill".into())
            .spawn(move || run(&shared, &data))
        {
            Ok(handle) => {
                let thread = alloc::boxed::Box::new(handle.thread().clone());
                Some(self.worker.get_or_init(|| thread))
            }
            Err(e) => {
                log::warn!("cache fill worker did not start, filling inline: {e}");
                self.inline.store(true, Ordering::Relaxed);
                None
            }
        }
    }
}

impl Drop for Fills {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
    }
}

/// The worker: drains the queue into the cache until the cache drops.
fn run(shared: &Shared, data: &Data) {
    let mut batch = Vec::new();
    let mut idle = 0;
    while !shared.closed.load(Ordering::Acquire) {
        {
            let mut queue = shared.queue.lock();
            core::mem::swap(&mut batch, &mut *queue);
            // Set under the lock, so a settle that finds the queue empty and
            // this flag clear knows every fill taken was made.
            shared.busy.store(!batch.is_empty(), Ordering::Release);
        }
        if batch.is_empty() {
            idle += 1;
            if idle < IDLE_POLLS {
                std::thread::sleep(POLL);
                continue;
            }
            shared.parked.store(true, Ordering::Release);
            // A fill pushed before the flag was seen set is picked up here,
            // so none waits for the next wake.
            if shared.queue.lock().is_empty() && !shared.closed.load(Ordering::Acquire) {
                std::thread::park();
            }
            shared.parked.store(false, Ordering::Release);
            idle = 0;
            continue;
        }
        idle = 0;
        #[expect(
            clippy::iter_with_drain,
            reason = "the batch keeps its allocation for the next drain"
        )]
        for fill in batch.drain(..) {
            fill.apply(data);
        }
        shared.busy.store(false, Ordering::Release);
        std::thread::sleep(POLL);
    }
}
