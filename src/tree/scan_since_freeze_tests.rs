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
    super::inner::TestHooks::install(
        &tree_for_hook.test_hooks.scan_freeze,
        Box::new(move || {
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
        }),
    );

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

/// The scan's writer exclusion works only if EVERY memtable write holds the
/// version-history read guard for its whole insert. A range deletion that
/// snapshots the memtable handle under a temporary guard and inserts after
/// releasing it would slip through the freeze: the tombstone could land while
/// (or right after) the capture runs, backdated at or below the returned
/// watermark, and a consumer that advanced past the watermark would lose the
/// deletion for good.
#[test]
fn a_range_deletion_holds_the_write_exclusion_guard_through_its_insert() {
    use crate::ScanSinceEvent;

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

    let tree = Arc::new(tree);
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (gate_tx, gate_rx) = mpsc::channel::<()>();

    // Fired between `remove_range` obtaining the memtable and inserting the
    // tombstone: park the writer there so the test can inspect the lock state
    // at the exact point the insert is about to run.
    super::inner::TestHooks::install(
        &tree.test_hooks.range_write,
        Box::new(move || {
            started_tx.send(()).ok();
            gate_rx.recv().expect("gate released");
        }),
    );

    let writer_tree = Arc::clone(&tree);
    let writer = std::thread::spawn(move || {
        // Backdated: at the cap, so the seqno bound would NOT exclude it.
        writer_tree.remove_range("a", "z", 10)
    });
    started_rx.recv().expect("writer reached its insert");

    // The writer is parked right before its insert. If it were not holding
    // the version-history read guard here, the scan's write guard would be
    // free to capture the memtable WITHOUT the tombstone — and then watch it
    // land backdated below the watermark it just returned.
    assert!(
        tree.version_history.try_write().is_none(),
        "a range-deletion writer must hold the version-history read guard \
         through its tombstone insert — the CDC freeze relies on it",
    );

    gate_tx.send(()).expect("release writer");
    writer.join().expect("writer thread");

    // End-to-end: with the guard spanning the insert, a scan starting after
    // the deletion returned sees it.
    let events: Vec<_> = tree.scan_since_seqno(0).expect("scan").collect::<Vec<_>>();
    assert!(
        events.iter().any(|e| matches!(
            e,
            ScanSinceEvent::RangeTombstone { start_key, end_key, seqno: 10 }
                if start_key.as_ref() == b"a" && end_key.as_ref() == b"z"
        )),
        "the committed range deletion must surface as a CDC event: {events:?}",
    );
}
