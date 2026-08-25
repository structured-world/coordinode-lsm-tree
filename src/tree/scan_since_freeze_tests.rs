// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! The CDC scan's snapshot contract against a CONCURRENT writer.

use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter, WriteBatch};
use std::sync::Arc;
use std::sync::mpsc;

/// The scan advertises a consistent snapshot of `[target, watermark]`, but the
/// seqno cap alone cannot deliver it: `apply_batch` takes the seqno from the
/// CALLER, so a write can commit at or below the cap after the cap was taken.
/// Walking the lock-free memtable live would then see that write or miss it
/// depending on where its node lands relative to the cursor — even splitting one
/// batch — and a consumer that advanced past the watermark would lose it for
/// good. The active memtable is therefore captured with writers excluded.
#[test]
fn a_concurrent_write_cannot_land_while_the_scan_captures_the_memtable() {
    let folder = tempfile::tempdir().expect("tempdir");
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected a standard tree");
    };
    tree.insert("k", "v", 10);

    let tree_for_hook = Arc::new(tree);
    let writer_tree = Arc::clone(&tree_for_hook);
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();

    // Fired while the scan holds the write guard: a writer started HERE must
    // still be blocked when we check, because the guard excludes it.
    super::scan_freeze_hook::install(Box::new(move || {
        let writer_tree = Arc::clone(&writer_tree);
        let done_tx = done_tx.clone();
        let started_tx = started_tx.clone();
        std::thread::spawn(move || {
            started_tx.send(()).ok();
            let mut batch = WriteBatch::new();
            // Backdated: at the cap, so the seqno bound would NOT exclude it.
            batch.insert("a", "concurrent");
            batch.insert("z", "concurrent");
            writer_tree.apply_batch(batch, 10).expect("apply");
            done_tx.send(()).ok();
        });
        // The writer has entered `apply_batch`; it cannot complete while this
        // hook runs, since the scan holds the guard its insert needs.
        started_rx.recv().expect("writer thread started");
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .is_err(),
            "a writer must not be able to commit while the scan captures the \
             active memtable — its entries would land mid-walk",
        );
    }));

    let events: Vec<_> = tree_for_hook
        .scan_since_seqno(0)
        .expect("scan")
        .collect::<Vec<_>>();

    // The write landed AFTER the capture, so it is wholly absent — never the
    // partial subset a live walk would have produced.
    let keys: Vec<_> = events.iter().map(|e| e.key().to_vec()).collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![b"k".to_vec()],
        "the frozen capture holds exactly the state at scan start: {events:?}",
    );
}
