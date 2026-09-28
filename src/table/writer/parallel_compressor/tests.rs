#![expect(clippy::expect_used, reason = "test code")]
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Deterministic spawner that runs each task synchronously on submit.
struct InlineSpawner;
impl CompactionSpawner for InlineSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

/// Keeps spawned tasks until asked to run them, counting how many were
/// spawned.
#[derive(Default)]
struct DeferredSpawner {
    tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    spawned: AtomicUsize,
}
impl DeferredSpawner {
    fn run_all(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("lock"));
        for task in tasks {
            task();
        }
    }
}
impl CompactionSpawner for DeferredSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.spawned.fetch_add(1, Ordering::SeqCst);
        self.tasks.lock().expect("lock").push(task);
    }
}

/// A compressor of plain (uncompressed, unencrypted) blocks.
fn plain(spawner: Arc<dyn CompactionSpawner>, inline_below: u64) -> BlockCompressor {
    BlockCompressor::new(
        spawner,
        2,
        4,
        inline_below,
        7,
        CompressionType::None,
        None,
        #[cfg(zstd_any)]
        None,
        #[cfg(zstd_any)]
        true,
        None,
    )
}

/// The uncompressed lengths of the blocks drained, in drain order: with no
/// compression each is the length of the payload submitted.
fn drained_lengths(c: &mut BlockCompressor) -> Vec<u32> {
    let mut lengths = Vec::new();
    while let Some(prepared) = c.take_next() {
        let prepared = prepared.expect("plain block prepares without error");
        let mut buf = Vec::new();
        let header = prepared
            .write_to(&mut buf, crate::table::block::ChecksumAt::Unbound)
            .expect("write to vec");
        lengths.push(header.uncompressed_length);
    }
    lengths
}

#[test]
fn take_next_returns_blocks_in_submission_order() {
    let mut c = plain(Arc::new(InlineSpawner), 0);
    assert_eq!(c.pending(), 0);
    assert!(c.take_next().is_none());

    c.submit(vec![0u8; 1], 0);
    c.submit(vec![0u8; 2], 0);
    c.submit(vec![0u8; 3], 0);
    assert_eq!(c.pending(), 3);
    assert_eq!(drained_lengths(&mut c), [1, 2, 3]);
    assert!(c.take_next().is_none());
}

#[test]
fn blocks_below_the_inline_threshold_never_reach_a_worker() {
    // Payloads under the threshold are prepared as they are submitted, on
    // the writer thread; only the larger ones are handed to a worker, and
    // every block still drains in submission order.
    let spawner = Arc::new(DeferredSpawner::default());
    let mut c = plain(spawner.clone(), 100);
    c.submit(vec![0u8; 10], 0);
    c.submit(vec![0u8; 99], 0);
    assert_eq!(spawner.spawned.load(Ordering::SeqCst), 0, "both ran inline");
    c.submit(vec![0u8; 100], 0);
    assert_eq!(
        spawner.spawned.load(Ordering::SeqCst),
        1,
        "a payload at the threshold goes to a worker",
    );
    spawner.run_all();
    assert_eq!(drained_lengths(&mut c), [10, 99, 100]);
}

#[test]
fn the_derived_threshold_is_a_share_of_the_block_length() {
    assert_eq!(
        derived_inline_below(4_096),
        4_096 / INLINE_BELOW_BLOCK_DIVISOR
    );
    assert_eq!(
        derived_inline_below(0),
        0,
        "no block length, nothing inline"
    );
}
