// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Regression: a flush running ON a thread of the caller-injected compaction
//! pool must complete. Its transform token tasks queue behind the flush itself
//! on the pool's only worker, so only the pipeline's help-first drain (running
//! queued jobs inline on the flushing thread) can finish it — the previous
//! park-only drain deadlocked here.

#![cfg(feature = "lz4")]
#![expect(clippy::expect_used, reason = "test assertions")]

use lsm_tree::config::CompressionPolicy;
use lsm_tree::table::writer::CompactionSpawner;
use lsm_tree::{AbstractTree, CompressionType, Config, SequenceNumberCounter};
use std::sync::{Arc, mpsc};

/// Single-worker executor: every spawned task runs sequentially on ONE thread,
/// so a task that blocks on a later-queued task can never be rescued by a
/// second worker.
struct SingleThread {
    tx: mpsc::Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl SingleThread {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Box<dyn FnOnce() + Send + 'static>>();
        std::thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                task();
            }
        });
        Self { tx }
    }
}

impl CompactionSpawner for SingleThread {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        let _ = self.tx.send(task);
    }
}

#[test]
fn flush_on_injected_pool_thread_completes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir().expect("tempdir");
    let spawner = Arc::new(SingleThread::new());

    // A real codec engages the parallel flush pipeline on the injected pool.
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
    .compaction_pool(Some(Arc::clone(&spawner) as Arc<dyn CompactionSpawner>))
    .open()?;

    for i in 0..10_000u64 {
        tree.insert(format!("key_{i:08}"), "x".repeat(128), i);
    }

    // Run the flush ON the pool's only worker: its block-transform tokens
    // queue behind this very call.
    let (done_tx, done_rx) = mpsc::channel();
    let flush_tree = tree.clone();
    spawner.spawn(Box::new(move || {
        let result = flush_tree.flush_active_memtable(0);
        let _ = done_tx.send(result);
    }));

    done_rx
        .recv_timeout(std::time::Duration::from_secs(45))
        .expect("flush deadlocked on its own injected pool")?;

    assert_eq!(tree.table_count(), 1);
    Ok(())
}
