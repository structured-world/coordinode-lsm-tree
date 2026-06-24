//! End-to-end crash-recovery test driving the engine through a torn manifest
//! commit with the fault-injection + crash-simulator backends.
//!
//! `FaultFs` makes the manifest-edit `fsync` of one flush fail; `CrashFs` then
//! discards the edit-log tail that failed `fsync` never made durable, modelling
//! a power loss at that instant. Reopening the engine on the post-crash store
//! must recover to the last durable state: the writes whose manifest commit was
//! acknowledged survive, the one whose commit never became durable is gone, and
//! recovery itself does not error on the orphaned table the failed flush left.

use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, MemFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn flush_with_torn_manifest_commit_recovers_to_last_durable_state() -> lsm_tree::Result<()> {
    // FaultFs (error injection) over CrashFs (durability tracking) over MemFs.
    // The CrashFs handle is cloned so we keep one to trigger crash()/inner()
    // while the tree owns its own (state- and store-sharing) clone.
    let crash = CrashFs::new(MemFs::new());
    let fault = FaultFs::new(crash.clone());
    let injector = fault.injector();

    let db = "/db";

    {
        let tree = lsm_tree::Config::new(
            db,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(fault))
        .open()?;

        // Two flushes whose manifest commits are acknowledged (durable).
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(0)?;

        // Arm a one-shot fault on the next edit-log fsync: the flush of "c"
        // writes and fsyncs its table, then fails to durably commit the manifest
        // edit. CrashFs therefore never records the edit-log's "c" tail as
        // durable.
        injector.arm(
            FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other))
                .on_path("edits")
                .once(),
        );
        tree.insert("c", "3", 2);
        assert!(
            tree.flush_active_memtable(0).is_err(),
            "the flush's manifest commit must surface the injected fsync failure"
        );
        // Drop the tree: release the directory lock and discard in-memory state.
    }

    // Power loss: roll every file back to its last fsync. The edit-log loses its
    // un-synced "c" tail; "c"'s table (fsynced before the manifest edit) survives
    // on disk as an orphan no committed version references.
    crash.crash();

    // Reopen on the raw post-crash store (no fault / crash wrapping).
    let tree = lsm_tree::Config::new(
        db,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(crash.inner())
    .open()?;

    assert!(
        tree.contains_key("a", u64::MAX)?,
        "the first acknowledged write survives the crash"
    );
    assert!(
        tree.contains_key("b", u64::MAX)?,
        "the second acknowledged write survives the crash"
    );
    assert!(
        !tree.contains_key("c", u64::MAX)?,
        "the write whose manifest commit never became durable is absent after recovery"
    );

    Ok(())
}
